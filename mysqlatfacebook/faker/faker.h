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

#ifndef FAKER_H_GUARD
#define FAKER_H_GUARD

#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <arpa/inet.h>
#include <string.h>
#include <ctype.h>


#include <glib.h>
#include <mysql.h>
#include <errmsg.h>

#include "binlog.h"

extern char *db_username, *db_password, *log_directory;
extern unsigned int db_port;

typedef struct {
    gboolean sql_running;
    gboolean io_running;
    int lag;
} SLAVE_STATUS;

MYSQL *init_slave(char *);
MYSQL_RES *slave_query(MYSQL *, char *, unsigned long);
void slave_query_noresult(MYSQL *, char *, unsigned long);
BINLOG *relaylog_from_slave(MYSQL *, BINLOG *);
char *slave_variable(MYSQL *, const char *);
void slave_sleep(MYSQL *, float, const char *);
void slave_status(MYSQL *, SLAVE_STATUS *);

void *sq_init(int max);
gpointer sq_pop(void *s);
void sq_push(void *s, gpointer);

#endif
