#ifndef IOT_SERVER_H
#define IOT_SERVER_H

#include <time.h>
#include <sys/time.h>
#include <sys/select.h>
#include <strings.h>

#define SERVER_PORT         9999
#define SERVER_IP           "127.0.0.1"
#define MAX_CLIENTS         10
#define BROADCAST_INTERVAL  5  /* seconds */
#define BUFFER_SIZE         256
#define BACKLOG             10
#define SELECT_TIMEOUT_SEC  1
#define SELECT_TIMEOUT_USEC 0

/* Simulated sensor parameter boundaries */
#define TEMP_BASE           25.0
#define TEMP_RANGE          10.0
#define HUMIDITY_BASE       30.0
#define HUMIDITY_RANGE      30.0

/* Operating modes */
#define MODE_IDLE           0
#define MODE_ACTIVE         1
#define MODE_ALERT          2

/* Client context structure */
typedef struct {
    int     fd;            /* Socket file descriptor (-1 if slot is free) */
    int     mode;          /* 0=idle, 1=active, 2=alert */
    time_t  last_activity; /* Timestamp of most recent activity */
    int     client_id;     /* 1-based sequential ID */
    char    ip[64];        /* Client IP string */
    int     port;          /* Client port */
} client_t;

#endif /* IOT_SERVER_H */
