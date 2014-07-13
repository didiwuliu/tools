CFLAGS+=`pkg-config glib-2.0 gthread-2.0 --cflags` `mysql_config --cflags` -std=gnu99 -Wall -Wextra -g -O2
LDFLAGS+=`pkg-config glib-2.0 gthread-2.0 --libs` `mysql_config --libs_r`

HEADERS=faker.h binlog.h
OBJECTS=faker.o binlog.o slave.o queue.o

all: faker

$(OBJECTS): $(HEADERS)
faker: $(OBJECTS)

clean:
	rm -f *.o *~ faker test-binlog

indent:
	indent -kr -ts4 -nut -l80 *.c *.h

test-binlog: binlog.o test-binlog.o
