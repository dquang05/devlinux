#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <poll.h>

#include "event_monitor.h"

#define FIFO_PERMS          0666
#define FILE_PERMS          0644
#define MONITOR_ACTIVE_INIT 1

/* Async-signal-safe termination flag */
static volatile sig_atomic_t g_running = 1;

/*
 * Signal handler strictly adhering to async-signal-safety.
 * Only modifies a volatile sig_atomic_t flag.
 */
static void sig_handler(int signo) {
    (void)signo;
    g_running = 0;
}

/*
 * Setup signal handlers for SIGTERM, SIGINT, and ignore SIGPIPE.
 * Returns 0 on success, -1 on error.
 */
static int setup_signals(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sa.sa_flags = 0;
    if (sigemptyset(&sa.sa_mask) < 0) {
        perror("sigemptyset");
        return -1;
    }

    if (sigaction(SIGTERM, &sa, NULL) < 0) {
        perror("sigaction SIGTERM");
        return -1;
    }

    if (sigaction(SIGINT, &sa, NULL) < 0) {
        perror("sigaction SIGINT");
        return -1;
    }

    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return -1;
    }

    return 0;
}

/*
 * Create FIFO if needed and open with O_RDWR to prevent EOF busy loops.
 * Returns file descriptor >= 0 on success, -1 on failure.
 */
static int init_fifo(const char *path) {
    if (mkfifo(path, FIFO_PERMS) < 0) {
        if (errno != EEXIST) {
            perror("mkfifo");
            return -1;
        }
    }

    /*
     * Open with O_RDWR | O_NONBLOCK:
     * Having our daemon hold a write reference avoids poll() returning EOF (POLLHUP)
     * whenever an external writer process closes its end after echo ... >> /tmp/event_log.
     */
    int fd = open(path, O_RDWR | O_NONBLOCK);
    if (fd < 0) {
        perror("open FIFO");
        return -1;
    }

    return fd;
}

/*
 * Create, bind, and listen on Unix Domain Socket.
 * Returns socket fd on success, -1 on error.
 */
static int init_unix_socket(const char *path) {
    if (unlink(path) < 0) {
        if (errno != ENOENT) {
            perror("unlink old socket");
        }
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket AF_UNIX");
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind AF_UNIX");
        if (close(fd) < 0) {
            perror("close");
        }
        return -1;
    }

    if (listen(fd, BACKLOG) < 0) {
        perror("listen AF_UNIX");
        if (close(fd) < 0) {
            perror("close");
        }
        return -1;
    }

    return fd;
}

/*
 * Open or create the monitored status file and record its initial size.
 * Returns file descriptor on success, -1 on error.
 */
static int init_monitored_file(const char *path, off_t *initial_size) {
    int fd = open(path, O_CREAT | O_RDWR, FILE_PERMS);
    if (fd < 0) {
        perror("open monitored file");
        return -1;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat monitored file");
        if (close(fd) < 0) {
            perror("close");
        }
        return -1;
    }

    *initial_size = st.st_size;
    return fd;
}

/*
 * Safe send loop to ensure all bytes are transmitted over socket.
 * Returns 0 on success, -1 on error.
 */
static int send_all(int fd, const char *buf, size_t len) {
    size_t total_sent = 0;

    if (fd < 0 || buf == NULL) {
        return -1;
    }

    while (total_sent < len) {
        ssize_t bytes_sent = send(fd, buf + total_sent, len - total_sent, MSG_NOSIGNAL);
        if (bytes_sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (bytes_sent == 0) {
            return -1;
        }
        total_sent += (size_t)bytes_sent;
    }
    return 0;
}

/*
 * Process messages read from FIFO.
 * Returns 0 on success, -1 on error.
 */
static int handle_fifo_event(int fifo_fd, int monitoring_active, long *total_events) {
    char buf[BUFFER_SIZE];
    memset(buf, 0, sizeof(buf));

    ssize_t bytes_read = read(fifo_fd, buf, sizeof(buf) - 1);
    if (bytes_read <= 0) {
        if (bytes_read < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                return 0;
            }
            perror("read FIFO");
            return -1;
        }
        return 0;
    }

    buf[bytes_read] = '\0';

    /* Parse line by line */
    char *saveptr = NULL;
    const char *line = strtok_r(buf, "\r\n", &saveptr);
    while (line != NULL) {
        if (line[0] != '\0') {
            if (monitoring_active) {
                if (printf("[FIFO_EVENT] %s\n", line) < 0) {
                    perror("printf");
                }
                if (fflush(stdout) != 0) {
                    perror("fflush");
                }
                (*total_events)++;
            }
        }
        line = strtok_r(NULL, "\r\n", &saveptr);
    }

    return 0;
}

/*
 * Handle control command from client connected via Unix Domain Socket.
 * Returns 0 to keep connection, 1 to close connection, -1 on error.
 */
static int process_control_command(int client_fd, const char *cmd, int *monitoring_active,
                                   long total_events, time_t start_time) {
    char response[BUFFER_SIZE];
    memset(response, 0, sizeof(response));

    if (strcmp(cmd, "STATUS") == 0) {
        time_t now = time(NULL);
        long uptime = 0;
        if (now != (time_t)-1) {
            uptime = (long)(now - start_time);
        }
        int len = snprintf(response, sizeof(response),
                           "Total events: %ld, Uptime: %ld seconds\n", total_events, uptime);
        if (len < 0 || (size_t)len >= sizeof(response)) {
            return -1;
        }
        if (send_all(client_fd, response, (size_t)len) < 0) {
            return 1;
        }
    } else if (strcmp(cmd, "START") == 0) {
        *monitoring_active = 1;
        const char *ok_msg = "OK\n";
        if (send_all(client_fd, ok_msg, strlen(ok_msg)) < 0) {
            return 1;
        }
    } else if (strcmp(cmd, "STOP") == 0) {
        *monitoring_active = 0;
        const char *ok_msg = "OK\n";
        if (send_all(client_fd, ok_msg, strlen(ok_msg)) < 0) {
            return 1;
        }
    } else if (strcasecmp(cmd, "EXIT") == 0) {
        return 1;
    } else {
        const char *err_msg = "ERROR: Unknown command\n";
        if (send_all(client_fd, err_msg, strlen(err_msg)) < 0) {
            return 1;
        }
    }

    return 0;
}

/*
 * Handle incoming client commands on client fd.
 * Returns 0 on success, 1 if client closed/exited, -1 on error.
 */
static int handle_client_activity(int client_fd, int *monitoring_active,
                                  long total_events, time_t start_time) {
    char buf[BUFFER_SIZE];
    memset(buf, 0, sizeof(buf));

    ssize_t bytes_recv = recv(client_fd, buf, sizeof(buf) - 1, 0);
    if (bytes_recv <= 0) {
        if (bytes_recv < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        }
        return 1;
    }

    buf[bytes_recv] = '\0';

    /* Trim trailing CR/LF */
    char *end = buf + bytes_recv - 1;
    while (end >= buf && (*end == '\r' || *end == '\n' || *end == ' ')) {
        *end = '\0';
        end--;
    }

    if (buf[0] == '\0') {
        return 0;
    }

    return process_control_command(client_fd, buf, monitoring_active, total_events, start_time);
}

/*
 * Check if the monitored file size has changed.
 * Returns 0 on success, -1 on error.
 */
static int check_file_change(const char *path, off_t *last_size, int monitoring_active, long *total_events) {
    struct stat st;
    if (stat(path, &st) < 0) {
        if (errno == ENOENT) {
            return 0;
        }
        perror("stat monitored file");
        return -1;
    }

    if (st.st_size != *last_size) {
        *last_size = st.st_size;
        if (monitoring_active) {
            if (printf("[FILE_EVENT] %s size changed to %ld bytes\n", path, (long)st.st_size) < 0) {
                perror("printf");
            }
            if (fflush(stdout) != 0) {
                perror("fflush");
            }
            (*total_events)++;
        }
    }

    return 0;
}

/*
 * Clean up all descriptors and files upon shutdown.
 * Returns 0 on success.
 */
static int cleanup_monitor(int fifo_fd, int sock_fd, int client_fd, int file_fd, const char *sock_path) {
    if (fifo_fd >= 0) {
        if (close(fifo_fd) < 0) {
            perror("close FIFO");
        }
    }

    if (client_fd >= 0) {
        if (close(client_fd) < 0) {
            perror("close client socket");
        }
    }

    if (sock_fd >= 0) {
        if (close(sock_fd) < 0) {
            perror("close listening socket");
        }
    }

    if (file_fd >= 0) {
        if (close(file_fd) < 0) {
            perror("close monitored file");
        }
    }

    if (unlink(sock_path) < 0) {
        if (errno != ENOENT) {
            perror("unlink socket");
        }
    }

    if (printf("[Monitor] Shutdown complete.\n") < 0) {
        perror("printf");
    }
    if (fflush(stdout) != 0) {
        perror("fflush");
    }

    return 0;
}

int main(void) {
    if (setvbuf(stdout, NULL, _IONBF, 0) != 0) {
        perror("setvbuf");
        return EXIT_FAILURE;
    }

    if (setup_signals() < 0) {
        return EXIT_FAILURE;
    }

    time_t start_time = time(NULL);
    if (start_time == (time_t)-1) {
        perror("time");
        start_time = 0;
    }

    int fifo_fd = init_fifo(FIFO_PATH);
    if (fifo_fd < 0) {
        return EXIT_FAILURE;
    }

    int sock_fd = init_unix_socket(SOCKET_PATH);
    if (sock_fd < 0) {
        if (close(fifo_fd) < 0) {
            perror("close");
        }
        return EXIT_FAILURE;
    }

    off_t last_file_size = 0;
    int file_fd = init_monitored_file(FILE_PATH, &last_file_size);
    if (file_fd < 0) {
        if (close(fifo_fd) < 0) {
            perror("close");
        }
        if (close(sock_fd) < 0) {
            perror("close");
        }
        return EXIT_FAILURE;
    }

    if (printf("[Monitor] Listening on %s\n", SOCKET_PATH) < 0) {
        perror("printf");
    }
    if (printf("[Monitor] Monitoring %s (FIFO) and %s\n", FIFO_PATH, FILE_PATH) < 0) {
        perror("printf");
    }
    if (fflush(stdout) != 0) {
        perror("fflush");
    }

    struct pollfd fds[NUM_POLL_FDS];
    fds[FD_FIFO].fd = fifo_fd;
    fds[FD_FIFO].events = POLLIN;

    fds[FD_SOCKET_LISTENER].fd = sock_fd;
    fds[FD_SOCKET_LISTENER].events = POLLIN;

    fds[FD_FILE].fd = file_fd;
    fds[FD_FILE].events = POLLERR; /* Just track descriptor, check via stat */

    int client_fd = -1;
    fds[FD_CLIENT].fd = -1;
    fds[FD_CLIENT].events = POLLIN;

    int monitoring_active = MONITOR_ACTIVE_INIT;
    long total_events = 0;

    while (g_running) {
        fds[FD_CLIENT].fd = client_fd;

        int poll_ret = poll(fds, NUM_POLL_FDS, POLL_TIMEOUT_MS);

        if (poll_ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("poll");
            break;
        }

        if (poll_ret == 0) {
            /* Heartbeat timeout */
            if (printf("[HEARTBEAT] Monitor alive, events_seen=%ld\n", total_events) < 0) {
                perror("printf");
            }
            if (fflush(stdout) != 0) {
                perror("fflush");
            }
        } else {
            /* 1. Check FIFO events */
            if (fds[FD_FIFO].revents & POLLIN) {
                if (handle_fifo_event(fifo_fd, monitoring_active, &total_events) < 0) {
                    perror("handle_fifo_event");
                }
            }

            /* 2. Check Socket listener for new connections */
            if (fds[FD_SOCKET_LISTENER].revents & POLLIN) {
                int new_client = accept(sock_fd, NULL, NULL);
                if (new_client >= 0) {
                    if (client_fd >= 0) {
                        /* Replace previous active client */
                        if (close(client_fd) < 0) {
                            perror("close old client");
                        }
                    }
                    client_fd = new_client;
                } else {
                    if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                        perror("accept");
                    }
                }
            }

            /* 3. Check active client for incoming commands */
            if (client_fd >= 0 && (fds[FD_CLIENT].revents & (POLLIN | POLLHUP | POLLERR))) {
                int res = handle_client_activity(client_fd, &monitoring_active, total_events, start_time);
                if (res != 0) {
                    if (close(client_fd) < 0) {
                        perror("close client socket");
                    }
                    client_fd = -1;
                }
            }
        }

        /* Check for changes in monitored file size */
        if (check_file_change(FILE_PATH, &last_file_size, monitoring_active, &total_events) < 0) {
            perror("check_file_change");
        }
    }

    if (cleanup_monitor(fifo_fd, sock_fd, client_fd, file_fd, SOCKET_PATH) < 0) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
