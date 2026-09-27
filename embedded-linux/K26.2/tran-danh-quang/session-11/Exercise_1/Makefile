CC = gcc
CFLAGS = -Wall -Wextra -pedantic -std=c99 -D_POSIX_C_SOURCE=200809L

TARGETS = iot_server iot_client

.PHONY: all clean

all: $(TARGETS)

iot_server: iot_server.c iot_server.h
	$(CC) $(CFLAGS) -o $@ iot_server.c

iot_client: iot_client.c iot_server.h
	$(CC) $(CFLAGS) -o $@ iot_client.c

clean:
	rm -f $(TARGETS) *.o
