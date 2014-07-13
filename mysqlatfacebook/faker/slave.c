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

/* Very very persistent attempts */
void connect_or_keep_connecting(MYSQL * mysql)
{
    while (!mysql_real_connect
           (mysql, "127.0.0.1", db_username, db_password, NULL,
            db_port, NULL, CLIENT_MULTI_STATEMENTS | CLIENT_REMEMBER_OPTIONS)) {
        g_warning("Couldn't connect to MySQL: %s", mysql_error(mysql));
        sleep(1);
    }
}

/* Initialize MySQL connection to a slave */
MYSQL *init_slave(char *init_command)
{
    /* Return always-ready MySQL connect handle */
    MYSQL *mysql = mysql_init(NULL);
    if (init_command)
        mysql_options(mysql, MYSQL_INIT_COMMAND, init_command);
    my_bool reconnect = 1;
    mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);
    connect_or_keep_connecting(mysql);
    return mysql;
}

/* Run a query, whenever server comes back up, return lats resultset data */
MYSQL_RES *slave_query(MYSQL * slave, char *query, unsigned long length)
{
    MYSQL_RES *res = NULL, *ret = NULL;
    int i = 0, status = 0;

    if (!length)
        length = strlen(query);
    while (mysql_real_query(slave, query, length)) {
        /* We will reconnect and try again,
           don't trust MySQL API reconnects */
        if (mysql_errno(slave) == CR_SERVER_LOST
            || mysql_errno(slave) == CR_CONN_HOST_ERROR)
            connect_or_keep_connecting(slave);
        else {
            g_warning("%s", mysql_error(slave));
            return NULL;
        }
        /* There were enough reconnects to prove ourselves
           that we get disconnected while running this query,
           oh noes. */
        if (i++ >= 3)
            return NULL;
    }

/* The very awesome MySQL C API boilerplate */

    do {
        res = mysql_store_result(slave);
        if (res) {
            if (ret)
                mysql_free_result(ret);
            ret = res;
        }
        if ((status = mysql_next_result(slave)) > 0)
            if (mysql_errno(slave) != 1180 && mysql_errno(slave) != 1062)
                g_warning("Could not execute statement: %s (%d)",
                          mysql_error(slave), mysql_errno(slave));
    } while (status == 0);
    return ret;
}



/* Opens relay log based on SLAVE STATUS and @@datadir */
BINLOG *relaylog_from_slave(MYSQL * slave, BINLOG * relaylog)
{
    MYSQL_RES *res = slave_query(slave, "SHOW SLAVE STATUS", 0);
    if (!res)
        return NULL;

    MYSQL_ROW row = mysql_fetch_row(res);
    BINLOG *bl;

    /* Does not have valid relay log */
    if (!row || !row[7] || !row[7][0]) {
        mysql_free_result(res);
        return NULL;
    }
    char *filename = row[7];
    guint32 position = atoi(row[8]);
    /* Not an absolute filename, need to construct some */
    if (row[7][0] != '/') {
        filename = g_strdup_printf("%s/%s", log_directory, filename);
    } else {
        filename = g_strdup(filename);
    }
    mysql_free_result(res);

    // No relay log known/passed
    if (!relaylog)
        bl = open_binlog(filename);

    // Different filename, must reopen
    else if (strcmp(filename, relaylog->filename)) {
        free_binlog(relaylog);
        bl = open_binlog(filename);
    } else {
        // Same file, we'll need just to seek
        bl = relaylog;
        g_free(filename);
    }

    if (bl)
        seek_binlog(bl, position);
    return bl;
}

/* Sleeps and tells why it is sleeping via MySQL processlist */
void slave_sleep(MYSQL * slave, float t, const char *message)
{
    char *query = g_strdup_printf("SELECT /* %s */ SLEEP(%f)", message, t);
    MYSQL_RES *res = slave_query(slave, query, 0);
    if (res)
        mysql_free_result(res);
    g_free(query);
}

/* Reports SLAVE STATUS to a structure specified */
void slave_status(MYSQL * slave, SLAVE_STATUS * st)
{
    st->io_running = 0;
    st->sql_running = 0;
    st->lag = 0;
    MYSQL_RES *res = slave_query(slave, "SHOW SLAVE STATUS", 0);
    if (!res)
        return;
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row) {
        if (row[10] && !strcmp(row[10], "Yes"))
            st->io_running = 1;
        if (row[11] && !strcmp(row[11], "Yes"))
            st->sql_running = 1;
        if (row[32]) {
            st->lag = atoi(row[32]);
            if (st->lag < 0)
                st->lag = 0;
        } else {
            /* Unknown lag may be lag too, if I/O thread is dead */
            st->lag = -1;
        }
    }
    mysql_free_result(res);
}
