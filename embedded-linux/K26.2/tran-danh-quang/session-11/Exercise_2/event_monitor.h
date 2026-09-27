#ifndef EVENT_MONITOR_H
#define EVENT_MONITOR_H

#include <poll.h>
#include <sys/types.h>

#define FIFO_PATH           "/tmp/event_log"
#define SOCKET_PATH         "/tmp/event_control.sock"
#define FILE_PATH           "/tmp/system_status"
#define POLL_TIMEOUT_MS     2000  /* 2 seconds */
#define BUFFER_SIZE         512
#define BACKLOG             5

typedef enum {
    FD_FIFO = 0,
    FD_SOCKET_LISTENER = 1,
    FD_FILE = 2,
    FD_CLIENT = 3,
    NUM_POLL_FDS = 4
} pollfd_index_t;

#endif /* EVENT_MONITOR_H */
