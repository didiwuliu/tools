/*
   Copyright 2012 Facebook

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "faker.h"

char *db_username = "root";
char *db_password = "";
char *log_directory = "/var/lib/mysql";

char *properties_file = NULL;

unsigned int db_port = 3306;

int prefetch_threshold = 1;
int prefetch_frequency = 10;
int prefetch_window_start = 2;
int prefetch_window_stop = 60;

int nthreads = 4;
gboolean debug, foreground, verbose;

void *queue;

void *worker(void *queue)
{
    EVENT *ev;
    mysql_thread_init();
    MYSQL *slave = init_slave("SET SESSION long_query_time=60, "
                              "innodb_fake_changes=1, sql_log_bin=0,"
                              "wait_timeout=5");

    GString *query = g_string_sized_new(100 * 1024);
    while ((ev = sq_pop(queue))) {
        // Build our query
        g_string_truncate(query, 0);
        g_string_append_printf(query, "USE %s;", ev->database);
        if (ev->insert_id)
            g_string_append_printf(query, "SET INSERT_ID=%llu;",
                                   (unsigned long long) ev->insert_id);
        if (ev->last_insert_id)
            g_string_append_printf(query, "SET LAST_INSERT_ID=%llu;",
                                   (unsigned long long)
                                   ev->last_insert_id);
        g_string_append_printf(query, "/* pos:%d */ ", ev->log_position);
        g_string_append_len(query, ev->query, ev->query_length);
        free_event(ev);

        if (debug) {
            printf("Would run: %*s\n", (int) query->len, query->str);
            slave_sleep(slave, 0.0001, "dry run");
            continue;
        }
        // Use our query
        MYSQL_RES *res = slave_query(slave, query->str, query->len);
        if (res)
            mysql_free_result(res);
    }
    mysql_close(slave);
    mysql_thread_end();
    return NULL;
}

gint64 get_monotonic_time()
{
    struct timespec tp;
    clock_gettime(CLOCK_MONOTONIC, &tp);
    return tp.tv_sec * 1000000 + tp.tv_nsec / 1000;
}


/* This skips comment blocks and gets to the first character in SQL query */
const char *skip_initial_comments(const char *query)
{
    char in_comment = 0, slash_before = 0, star_before = 0;
    const char *p;

    for (p = query; *p; p++) {
        if (isspace(*p))
            continue;
        if (!in_comment) {
            if (isalpha(*p))
                return p;
            if (slash_before && *p == '*') {
                in_comment = 1;
            }
            slash_before = (*p == '/');
        } else {
            if (star_before && *p == '/') {
                in_comment = 0;
            }
            star_before = (*p == '*');
        }
    }
    return *p ? p : NULL;
}

gboolean valid_event(EVENT * event)
{
    const char *prefix = skip_initial_comments(event->query);
    if (!prefix)
        return FALSE;
    if (!strncasecmp(prefix, "UPDATE", sizeof("UPDATE") - 1) ||
        !strncasecmp(prefix, "INSERT", sizeof("INSERT") - 1) ||
        !strncasecmp(prefix, "DELETE", sizeof("DELETE") - 1)
        || !strncasecmp(prefix, "REPLACE", sizeof("REPLACE") - 1))
        return TRUE;
    else
        return FALSE;
}

static GOptionEntry entries[] = {
    {"port", 'P', 0, G_OPTION_ARG_INT, &db_port, "MySQL port number", NULL},
    {"username", 'u', 0, G_OPTION_ARG_STRING, &db_username, "MySQL username",
     NULL},
    {"password", 'p', 0, G_OPTION_ARG_STRING, &db_password, "MySQL password",
     NULL},
    {"threads", 't', 0, G_OPTION_ARG_INT, &nthreads,
     "Number of parallel threads", NULL},
    {"debug", 'd', 0, G_OPTION_ARG_NONE, &debug, "Debug (dry run) mode", NULL},
    {"verbose", 'v', 0, G_OPTION_ARG_NONE, &verbose,
     "Print lots of verbose stuff", NULL},
    {"foreground", 'F', 0, G_OPTION_ARG_NONE, &foreground,
     "Run in foreground, don't daemonize", NULL},
    {"properties", 'f', 0, G_OPTION_ARG_FILENAME, &properties_file,
     "File with authentication properties", NULL},
    {"threshold", 0, 0, G_OPTION_ARG_INT, &prefetch_threshold,
     "Lag threshold at which to prefetch", NULL},
    {"frequency", 0, 0, G_OPTION_ARG_INT, &prefetch_frequency,
     "Frequency (hz) how often to check for lag", NULL},
    {"window-start", 0, 0, G_OPTION_ARG_INT, &prefetch_window_start,
     "Window offset (in seconds) at which to prefetch", NULL},
    {"window-stop", 0, 0, G_OPTION_ARG_INT, &prefetch_window_stop,
     "Window offset (in seconds) at which to stop prefetching", NULL},
    {NULL, 0, 0, G_OPTION_ARG_NONE, NULL, NULL, NULL}
};

int main(int ac, char **av)
{
    BINLOG *relaylog = NULL;
    EVENT *event;
    SLAVE_STATUS st;

    GError *error = NULL;

    char *prefetched_file = NULL;
    guint prefetched_pos = 0;
    guint cycles = 0;

    g_thread_init(NULL);
    queue = sq_init(50);

    mysql_library_init(0, NULL, NULL);
    mysql_thread_init();

    GOptionContext *context =
        g_option_context_new("fake changes based replication prefetcher");
    g_option_context_add_main_entries(context, entries, NULL);
    if (!g_option_context_parse(context, &ac, &av, &error)) {
        g_print("option parsing failed: %s, try --help\n", error->message);
        exit(EXIT_FAILURE);
    }
    g_option_context_free(context);

    // We allow k/v file passed for configuration
    if (properties_file) {
        FILE *pf = fopen(properties_file, "r");
        char lb[128];
        char *key, *value;

        if (!pf) {
            g_print("Could not open properties file\n");
            exit(EXIT_FAILURE);
        }

        while (fgets(lb, sizeof(lb) - 1, pf)) {
            key = strtok(lb, " \t\n");
            value = strtok(NULL, " \t\n");

            if (!key || !value)
                continue;

            if (!strcmp(key, "mysql_user"))
                db_username = strdup(value);
            else if (!strcmp(key, "mysql_pass"))
                db_password = strdup(value);
        }
        fclose(pf);
    }

    MYSQL *slave = init_slave("SET SESSION long_query_time=60, "
                              "innodb_fake_changes=1");

    if (!foreground && !daemon(0, 0))
        g_warning("Couldn't daemonize");


    while (nthreads--)
        g_thread_create(worker, queue, 0, NULL);

    gboolean skip_event = FALSE;

    for (;;) {
        slave_status(slave, &st);
        if (!st.sql_running) {
            slave_sleep(slave, 10, "Waiting for replication");
            continue;
        }

        if (st.lag > 0 && st.lag <= prefetch_threshold) {
            if (verbose)
                printf("Lag (%d) is below threshold\n", st.lag);

            slave_sleep(slave, 1.0 / prefetch_frequency,
                        "Lag is below threshold");
            continue;
        }

        relaylog = relaylog_from_slave(slave, relaylog);
        if (!relaylog) {
            slave_sleep(slave, 1.0 / prefetch_frequency, "Log ran away");
            continue;
        }

        event = read_binlog(relaylog);
        if (!event) {
            if (verbose)
                printf("No more events returned from relay log, "
                       "prefetched position: %d \n", prefetched_pos);

            slave_sleep(slave, 1.0 / prefetch_frequency,
                        "Reached the end of binlog");
            continue;
        }

        time_t sql_time = event->timestamp;

        // Save position of a new file
        if (!prefetched_file || strcmp(relaylog->filename, prefetched_file)) {
            if (verbose)
                printf("Looking at a new (%s)  file instead of old one (%s)\n",
                       relaylog->filename, prefetched_file);

            if (prefetched_file)
                free(prefetched_file);
            prefetched_file = strdup(relaylog->filename);
            prefetched_pos = relaylog->position;
        } else if (prefetched_pos > relaylog->position) {
            // We allow ourselves to jump to position ahead, if filename matches
            if (verbose)
                printf("Jumping to %s:%d\n", prefetched_file, prefetched_pos);
            seek_binlog(relaylog, prefetched_pos);
        }
        if (verbose)
            printf("Starting binlog read at %d, prefetched at : %d\n",
                   relaylog->position, prefetched_pos);

        gboolean nosleep = FALSE;
        while ((event = read_binlog(relaylog))) {
            prefetched_pos = event->log_position;
            // Skip too short or too early events
            if (event->timestamp < sql_time + prefetch_window_start) {
                if (verbose)
                    printf("Event timestamp (%d) is too close to "
                           "replication thread timestamp (%d)\n",
                           (int) event->timestamp, (int) sql_time);
                free_event(event);
                continue;
            }
            // Break from the loop if events are too far
            if (event->timestamp > sql_time + prefetch_window_stop) {
                if (verbose)
                    printf("Event timestamp (%d) is way beyond replication "
                           "thread timestamp (%d)\n",
                           (int) event->timestamp, (int) sql_time);
                free_event(event);
                break;
            }
            // Validate event by known prefixes
            if (!valid_event(event)) {
                free_event(event);
                continue;
            }
            // We may want to ignore certain events at certain cases
            // like reaching end of log
            if (skip_event) {
                skip_event = FALSE;
                free_event(event);
                continue;
            }
            // We go async here!
            gint64 start = get_monotonic_time();
            if (verbose)
                printf("Executing event at %s:%d\n", relaylog->filename,
                       event->log_position);
            sq_push(queue, event);

            // Queue blocked us for too long, or too many events. re-evaluate!
            if (get_monotonic_time() - start > 2000000 || !(++cycles % 1000)) {
                if (verbose)
                    printf("Blocked for too long or cycle limits kicked in\n");

                nosleep = TRUE;
                break;
            }
        }

        if (!nosleep) {
            char *sleep_state =
                g_strdup_printf("Got ahead to %d", prefetched_pos);
            slave_sleep(slave, 1.0 / prefetch_frequency, sleep_state);
            g_free(sleep_state);
        }
        if (verbose)
            printf("Reached end of relay log at %s:%d\n", relaylog->filename,
                   prefetched_pos);

        // There's nothing interesting at this position, let's skip it afterwards
        skip_event = TRUE;
    }

}
