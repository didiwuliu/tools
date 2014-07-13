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

#ifndef BINLOG_H_GUARD
#define BINLOG_H_GUARD

#define MAX_EVENT_SIZE 1024*1024

enum Log_event_type {
    UNKNOWN_EVENT = 0,
    START_EVENT_V3 = 1,
    QUERY_EVENT = 2,
    STOP_EVENT = 3,
    ROTATE_EVENT = 4,
    INTVAR_EVENT = 5,
    FORMAT_DESCRIPTION_EVENT = 15
};

#pragma pack(1)
struct FDE {
    guint32 timestamp;
    guint8 event_type;
    guint32 server_id;
    guint32 event_length;
    guint32 next_position;
    guint16 flags;
    guint16 binlog_version;
    char serverversion[50];
    guint32 create_timestamp;
    guint8 header_length;
};

struct EH {
    guint32 timestamp;
    guint8 event_type;
    guint32 server_id;
    guint32 event_length;
    guint32 next_position;
    guint16 flags;
};

struct QEH {
    guint32 thread_id;
    guint32 elapsed;
    guint8 db_length;
    guint16 error_code;
    guint16 status_length;
};

struct IntvarEvent {
    guint8 type;
    guint64 value;
};

#pragma pack()

typedef struct {
    char *filename;
    FILE *fd;
    struct FDE fde;
    guint32 position;
    guint64 insert_id;
    guint64 last_insert_id;
    void *buffer;
    void *vbuffer;
    guint8 *header_lengths;
    int header_lengths_length;
    int checksum_algo;
    int checksum_size;
} BINLOG;

typedef struct {
    char *log_file;
    guint32 log_position;
    guint32 timestamp;
    char *database;
    void *query;
    guint32 query_length;

    guint64 insert_id;
    guint64 last_insert_id;
} EVENT;

BINLOG *open_binlog(char *filename);
EVENT *read_binlog(BINLOG *);

void seek_binlog(BINLOG *, guint32);

void free_event(EVENT *);
void free_binlog(BINLOG *);

#endif
