#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "event_monitor.h"

/* Signal flag for graceful shutdown */
static volatile sig_atomic_t g_running = 1;

/*
 * Async-signal-safe signal handler.
 */
static void sig_handler(int signo) {
    (void)signo;
    g_running = 0;
}

/*
 * Setup signal handling with sigaction.
 * Returns 0 on success, -1 on failure.
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
 * Connect to Unix domain control socket.
 * Returns socket fd on success, -1 on error.
 */
static int connect_to_monitor(const char *sock_path) {
    int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        perror("socket AF_UNIX");
        return -1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);
    addr.sun_path[sizeof(addr.sun_path) - 1] = '\0';

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect to event monitor socket");
        if (close(sock_fd) < 0) {
            perror("close");
        }
        return -1;
    }

    return sock_fd;
}

/*
 * Interactive command loop for client.
 * Returns 0 on success, -1 on failure.
 */
static int run_client_loop(int sock_fd) {
    char in_buf[BUFFER_SIZE];
    char recv_buf[BUFFER_SIZE];

    while (g_running) {
        memset(in_buf, 0, sizeof(in_buf));
        if (fgets(in_buf, sizeof(in_buf), stdin) == NULL) {
            /* EOF reached */
            (void)send_all(sock_fd, "EXIT\n", 5);
            break;
        }

        /* Check if user wants to exit */
        if (strncasecmp(in_buf, "exit", 4) == 0 &&
            (in_buf[4] == '\n' || in_buf[4] == '\r' || in_buf[4] == '\0')) {
            (void)send_all(sock_fd, "EXIT\n", 5);
            break;
        }

        if (send_all(sock_fd, in_buf, strlen(in_buf)) < 0) {
            perror("send command");
            return -1;
        }

        memset(recv_buf, 0, sizeof(recv_buf));
        ssize_t bytes_recv = recv(sock_fd, recv_buf, sizeof(recv_buf) - 1, 0);
        if (bytes_recv <= 0) {
            if (bytes_recv < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
                continue;
            }
            break;
        }

        recv_buf[bytes_recv] = '\0';
        if (printf("%s", recv_buf) < 0) {
            perror("printf");
        }
        if (fflush(stdout) != 0) {
            perror("fflush");
        }
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

    int sock_fd = connect_to_monitor(SOCKET_PATH);
    if (sock_fd < 0) {
        return EXIT_FAILURE;
    }

    int res = run_client_loop(sock_fd);

    if (close(sock_fd) < 0) {
        perror("close socket");
        return EXIT_FAILURE;
    }

    if (res < 0) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
