CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c99 -D_POSIX_C_SOURCE=200809L

TARGETS = event_monitor event_client

.PHONY: all clean

all: $(TARGETS)

event_monitor: event_monitor.c event_monitor.h
	$(CC) $(CFLAGS) -o $@ event_monitor.c

event_client: event_client.c event_monitor.h
	$(CC) $(CFLAGS) -o $@ event_client.c

clean:
	rm -f $(TARGETS) *.o
