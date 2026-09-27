#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "iot_server.h"

#define MAX_PENDING_CONNECTIONS 10
#define CLIENT_ID_START         1
#define SENSOR_RAND_SCALE       10.0
#define SENSOR_TEMP_MOD         100
#define SENSOR_HUMID_MOD        300

/* Signal flag for graceful shutdown */
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
 * Safe send loop to ensure all bytes are transmitted.
 * Uses MSG_NOSIGNAL to prevent SIGPIPE if a client disconnects.
 * Returns 0 on success, -1 on failure.
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
 * Initialize client slot array.
 * Returns 0 on success.
 */
static int init_clients(client_t clients[], size_t count) {
    size_t i;
    for (i = 0; i < count; ++i) {
        clients[i].fd = -1;
        clients[i].mode = MODE_IDLE;
        clients[i].last_activity = 0;
        clients[i].client_id = 0;
        clients[i].ip[0] = '\0';
        clients[i].port = 0;
    }
    return 0;
}

/*
 * Find an empty slot in client array.
 * Returns slot index >= 0, or -1 if full.
 */
static int find_free_client_slot(const client_t clients[], size_t count) {
    size_t i;
    for (i = 0; i < count; ++i) {
        if (clients[i].fd == -1) {
            return (int)i;
        }
    }
    return -1;
}

/*
 * Count total active clients.
 * Returns count >= 0.
 */
static int count_active_clients(const client_t clients[], size_t count) {
    size_t i;
    int active = 0;
    for (i = 0; i < count; ++i) {
        if (clients[i].fd != -1) {
            active++;
        }
    }
    return active;
}

/*
 * Get highest mode amongst all connected clients.
 * Returns mode value.
 */
static int get_max_client_mode(const client_t clients[], size_t count) {
    size_t i;
    int max_mode = MODE_IDLE;
    for (i = 0; i < count; ++i) {
        if (clients[i].fd != -1 && clients[i].mode > max_mode) {
            max_mode = clients[i].mode;
        }
    }
    return max_mode;
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

    /* Ignore SIGPIPE so abrupt client drops don't kill server */
    sa.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &sa, NULL) < 0) {
        perror("sigaction SIGPIPE");
        return -1;
    }

    return 0;
}

/*
 * Create, configure, bind and listen on TCP server socket.
 * Returns server fd on success, -1 on failure.
 */
static int setup_server_socket(int port) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return -1;
    }

    int optval = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        perror("setsockopt SO_REUSEADDR");
        if (close(server_fd) < 0) {
            perror("close");
        }
        return -1;
    }

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons((uint16_t)port);

    if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        if (close(server_fd) < 0) {
            perror("close");
        }
        return -1;
    }

    if (listen(server_fd, MAX_PENDING_CONNECTIONS) < 0) {
        perror("listen");
        if (close(server_fd) < 0) {
            perror("close");
        }
        return -1;
    }

    return server_fd;
}

/*
 * Handle incoming connection on server socket.
 * Returns 0 on success, -1 on failure.
 */
static int handle_new_connection(int server_fd, client_t clients[], size_t max_clients, int *next_id) {
    struct sockaddr_in cli_addr;
    socklen_t cli_len = sizeof(cli_addr);
    int client_fd = accept(server_fd, (struct sockaddr *)&cli_addr, &cli_len);

    if (client_fd < 0) {
        if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0;
        }
        perror("accept");
        return -1;
    }

    int slot = find_free_client_slot(clients, max_clients);
    if (slot < 0) {
        const char *full_msg = "ERROR: Server full\n";
        (void)send_all(client_fd, full_msg, strlen(full_msg));
        if (close(client_fd) < 0) {
            perror("close");
        }
        return 0;
    }

    clients[slot].fd = client_fd;
    clients[slot].mode = MODE_IDLE;
    clients[slot].client_id = (*next_id)++;
    time_t now = time(NULL);
    if (now == (time_t)-1) {
        perror("time");
        clients[slot].last_activity = 0;
    } else {
        clients[slot].last_activity = now;
    }

    if (inet_ntop(AF_INET, &cli_addr.sin_addr, clients[slot].ip, sizeof(clients[slot].ip)) == NULL) {
        strncpy(clients[slot].ip, "unknown", sizeof(clients[slot].ip) - 1);
        clients[slot].ip[sizeof(clients[slot].ip) - 1] = '\0';
    }
    clients[slot].port = (int)ntohs(cli_addr.sin_port);

    if (printf("[Server] Client %d connected from %s:%d\n",
               clients[slot].client_id, clients[slot].ip, clients[slot].port) < 0) {
        perror("printf");
    }
    if (fflush(stdout) != 0) {
        perror("fflush");
    }

    return 0;
}

/*
 * Parse and process a single command line from a client.
 * Returns 0 on success, 1 if client disconnected/quit, -1 on fatal error.
 */
static int process_client_command(client_t *client, const char *cmd) {
    char response[BUFFER_SIZE];
    memset(response, 0, sizeof(response));

    if (strcmp(cmd, "GET_TEMP") == 0) {
        double temp = TEMP_BASE + (double)(rand() % SENSOR_TEMP_MOD) / SENSOR_RAND_SCALE;
        int ret = snprintf(response, sizeof(response), "%.1f\n", temp);
        if (ret < 0 || (size_t)ret >= sizeof(response)) {
            return -1;
        }
        if (printf("[Server] Client %d: GET_TEMP → %.1f\n", client->client_id, temp) < 0) {
            perror("printf");
        }
        if (fflush(stdout) != 0) {
            perror("fflush");
        }
        if (send_all(client->fd, response, strlen(response)) < 0) {
            return 1;
        }
    } else if (strcmp(cmd, "GET_HUMIDITY") == 0) {
        double humidity = HUMIDITY_BASE + (double)(rand() % SENSOR_HUMID_MOD) / SENSOR_RAND_SCALE;
        int ret = snprintf(response, sizeof(response), "%.1f\n", humidity);
        if (ret < 0 || (size_t)ret >= sizeof(response)) {
            return -1;
        }
        if (printf("[Server] Client %d: GET_HUMIDITY → %.1f\n", client->client_id, humidity) < 0) {
            perror("printf");
        }
        if (fflush(stdout) != 0) {
            perror("fflush");
        }
        if (send_all(client->fd, response, strlen(response)) < 0) {
            return 1;
        }
    } else if (strncmp(cmd, "SET_MODE", 8) == 0) {
        int new_mode = -1;
        if (sscanf(cmd, "SET_MODE %d", &new_mode) == 1 &&
            (new_mode == MODE_IDLE || new_mode == MODE_ACTIVE || new_mode == MODE_ALERT)) {
            client->mode = new_mode;
            int ret = snprintf(response, sizeof(response), "OK\n");
            if (ret < 0 || (size_t)ret >= sizeof(response)) {
                return -1;
            }
            if (printf("[Server] Client %d: SET_MODE %d → OK\n", client->client_id, new_mode) < 0) {
                perror("printf");
            }
            if (fflush(stdout) != 0) {
                perror("fflush");
            }
            if (send_all(client->fd, response, strlen(response)) < 0) {
                return 1;
            }
        } else {
            const char *err_msg = "ERROR: Invalid mode\n";
            if (send_all(client->fd, err_msg, strlen(err_msg)) < 0) {
                return 1;
            }
        }
    } else if (strcasecmp(cmd, "QUIT") == 0) {
        return 1;
    } else {
        const char *err_msg = "ERROR: Unknown command\n";
        if (printf("[Server] Client %d: Unknown command '%s'\n", client->client_id, cmd) < 0) {
            perror("printf");
        }
        if (fflush(stdout) != 0) {
            perror("fflush");
        }
        if (send_all(client->fd, err_msg, strlen(err_msg)) < 0) {
            return 1;
        }
    }

    return 0;
}

/*
 * Handle incoming data from an existing client socket.
 * Returns 0 on success, -1 on failure.
 */
static int handle_client_data(client_t *client) {
    char buf[BUFFER_SIZE];
    memset(buf, 0, sizeof(buf));

    ssize_t bytes_read = recv(client->fd, buf, sizeof(buf) - 1, 0);
    if (bytes_read <= 0) {
        if (bytes_read < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) {
            return 0;
        }
        /* Client disconnected or error */
        if (printf("[Server] Client %d disconnected.\n", client->client_id) < 0) {
            perror("printf");
        }
        if (fflush(stdout) != 0) {
            perror("fflush");
        }
        if (close(client->fd) < 0) {
            perror("close client socket");
        }
        client->fd = -1;
        return 0;
    }

    buf[bytes_read] = '\0';
    time_t now = time(NULL);
    if (now != (time_t)-1) {
        client->last_activity = now;
    }

    /* Parse command line by line */
    char *saveptr = NULL;
    char *line = strtok_r(buf, "\r\n", &saveptr);
    while (line != NULL) {
        /* Trim leading/trailing whitespace */
        while (*line == ' ' || *line == '\t') {
            line++;
        }
        size_t line_len = strlen(line);
        while (line_len > 0 && (line[line_len - 1] == ' ' || line[line_len - 1] == '\t')) {
            line[line_len - 1] = '\0';
            line_len--;
        }

        if (line[0] != '\0') {
            int res = process_client_command(client, line);
            if (res == 1) {
                if (close(client->fd) < 0) {
                    perror("close client socket");
                }
                client->fd = -1;
                return 0;
            } else if (res < 0) {
                return -1;
            }
        }
        line = strtok_r(NULL, "\r\n", &saveptr);
    }

    return 0;
}

/*
 * Broadcast periodic status message to all active clients.
 * Returns 0 on success, -1 on error.
 */
static int broadcast_status(client_t clients[], size_t max_clients) {
    int active_clients = count_active_clients(clients, max_clients);
    if (active_clients <= 0) {
        return 0;
    }

    double temp = TEMP_BASE + (double)(rand() % SENSOR_TEMP_MOD) / SENSOR_RAND_SCALE;
    double humidity = HUMIDITY_BASE + (double)(rand() % SENSOR_HUMID_MOD) / SENSOR_RAND_SCALE;
    int mode = get_max_client_mode(clients, max_clients);

    if (printf("[Server] Broadcasting to %d clients: Temp=%.1f Humidity=%.1f Mode=%d Clients=%d\n",
               active_clients, temp, humidity, mode, active_clients) < 0) {
        perror("printf");
    }
    if (fflush(stdout) != 0) {
        perror("fflush");
    }

    char msg[BUFFER_SIZE];
    int len = snprintf(msg, sizeof(msg),
                       "[BROADCAST] Temp=%.1f Humidity=%.1f Mode=%d Clients=%d\n",
                       temp, humidity, mode, active_clients);
    if (len < 0 || (size_t)len >= sizeof(msg)) {
        return -1;
    }

    size_t i;
    for (i = 0; i < max_clients; ++i) {
        if (clients[i].fd != -1) {
            if (send_all(clients[i].fd, msg, (size_t)len) < 0) {
                if (close(clients[i].fd) < 0) {
                    perror("close dropped client");
                }
                clients[i].fd = -1;
            }
        }
    }

    return 0;
}

/*
 * Close all active client connections and server socket.
 * Returns 0 on success.
 */
static int cleanup_server(int server_fd, client_t clients[], size_t max_clients) {
    size_t i;
    for (i = 0; i < max_clients; ++i) {
        if (clients[i].fd != -1) {
            if (close(clients[i].fd) < 0) {
                perror("close client socket");
            }
            clients[i].fd = -1;
        }
    }

    if (server_fd >= 0) {
        if (close(server_fd) < 0) {
            perror("close server socket");
        }
    }

    if (printf("[Server] Shutdown complete.\n") < 0) {
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

    srand((unsigned int)time(NULL));

    if (setup_signals() < 0) {
        return EXIT_FAILURE;
    }

    client_t clients[MAX_CLIENTS];
    if (init_clients(clients, MAX_CLIENTS) < 0) {
        return EXIT_FAILURE;
    }

    int server_fd = setup_server_socket(SERVER_PORT);
    if (server_fd < 0) {
        return EXIT_FAILURE;
    }

    if (printf("[Server] Listening on localhost:%d\n", SERVER_PORT) < 0) {
        perror("printf");
    }
    if (fflush(stdout) != 0) {
        perror("fflush");
    }

    time_t last_broadcast = time(NULL);
    if (last_broadcast == (time_t)-1) {
        perror("time");
        last_broadcast = 0;
    }

    int next_client_id = CLIENT_ID_START;

    while (g_running) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(server_fd, &readfds);
        int max_fd = server_fd;

        size_t i;
        for (i = 0; i < MAX_CLIENTS; ++i) {
            if (clients[i].fd != -1) {
                FD_SET(clients[i].fd, &readfds);
                if (clients[i].fd > max_fd) {
                    max_fd = clients[i].fd;
                }
            }
        }

        struct timeval tv;
        tv.tv_sec = SELECT_TIMEOUT_SEC;
        tv.tv_usec = SELECT_TIMEOUT_USEC;

        int activity = select(max_fd + 1, &readfds, NULL, NULL, &tv);

        if (activity < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("select");
            break;
        }

        /* Check for new connection */
        if (activity > 0 && FD_ISSET(server_fd, &readfds)) {
            if (handle_new_connection(server_fd, clients, MAX_CLIENTS, &next_client_id) < 0) {
                break;
            }
        }

        /* Check client sockets */
        if (activity > 0) {
            for (i = 0; i < MAX_CLIENTS; ++i) {
                if (clients[i].fd != -1 && FD_ISSET(clients[i].fd, &readfds)) {
                    if (handle_client_data(&clients[i]) < 0) {
                        break;
                    }
                }
            }
        }

        /* Periodic broadcast */
        time_t current_time = time(NULL);
        if (current_time != (time_t)-1) {
            if (current_time - last_broadcast >= BROADCAST_INTERVAL) {
                if (broadcast_status(clients, MAX_CLIENTS) < 0) {
                    fprintf(stderr, "Broadcast failed\n");
                }
                last_broadcast = current_time;
            }
        }
    }

    if (cleanup_server(server_fd, clients, MAX_CLIENTS) < 0) {
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
