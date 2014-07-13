#include "faker.h"

int main(int argc, char **argv)
{

    if (argc < 2) {
        g_critical("Need binlog file name");
        exit(EXIT_FAILURE);
    }

    BINLOG *binlog = open_binlog(argv[1]);
    EVENT *event;

    if (!binlog)
        exit(EXIT_FAILURE);

    while ((event = read_binlog(binlog))) {
        printf("%*s\n", event->query_length, (char *) event->query);
    }
}
