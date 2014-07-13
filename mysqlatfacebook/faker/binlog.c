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

#define BINLOG_BUFFER 1024*1024

BINLOG *open_binlog(char *filename)
{
    BINLOG *binlog;
    FILE *fd = fopen(filename, "r");
    if (!fd) {
        g_warning("Couldn't open binlog (%d)", errno);
        return NULL;
    }


    binlog = g_new0(BINLOG, 1);
    binlog->fd = fd;
    binlog->buffer = malloc(MAX_EVENT_SIZE);
    binlog->vbuffer = malloc(BINLOG_BUFFER);
    setvbuf(fd, binlog->vbuffer, _IOFBF, BINLOG_BUFFER);

    guint32 magic;
    if (!fread(&magic, sizeof(magic), 1, binlog->fd)
        || magic != 1852400382)
        goto err;

    if (!fread(&binlog->fde, sizeof(binlog->fde), 1, binlog->fd)
        || binlog->fde.event_type != FORMAT_DESCRIPTION_EVENT
        || binlog->fde.binlog_version != 4)
        goto err;

    int blhl = binlog->fde.event_length - sizeof(binlog->fde);
    binlog->header_lengths_length = blhl;
    binlog->header_lengths = malloc(blhl);

    if (!fread(binlog->header_lengths, blhl, 1, binlog->fd))
        goto err;

    // None on pre-5.6 condition, we default to 4 bytes for all unknown algos
    binlog->checksum_algo = blhl < 40 ? 0 : binlog->header_lengths[blhl - 5];

    if (binlog->checksum_algo)
        binlog->checksum_size = 4;

    binlog->filename = filename;
    binlog->position = binlog->fde.next_position;

    fseek(binlog->fd, binlog->fde.next_position, SEEK_SET);
    return binlog;

  err:
    g_warning("Invalid binlog");
    if (binlog->fd)
        fclose(binlog->fd);
    free(binlog->buffer);
    free(binlog->vbuffer);
    g_free(binlog);
    return NULL;
}

EVENT *read_binlog(BINLOG * binlog)
{
    struct EH eh;
    struct QEH *qeh;
    struct IntvarEvent *iv;
    EVENT *event;

    for (;;) {
        if (!fread(&eh, sizeof(eh), 1, binlog->fd))
            return NULL;

        guint32 body_length = eh.event_length - binlog->fde.header_length;

        // We will skip events that are way too large for us
        if (body_length > MAX_EVENT_SIZE) {
            fseek(binlog->fd, body_length, SEEK_CUR);
            continue;
        }

        if (!fread(binlog->buffer, body_length, 1, binlog->fd))
            return NULL;

        switch (eh.event_type) {
        case QUERY_EVENT:
            /* Figure out where in binlog buffer query and database sit */
            qeh = binlog->buffer;
            char *db = binlog->buffer + sizeof(*qeh) + qeh->status_length;
            void *query = db + qeh->db_length + 1;
            guint32 query_length =
                body_length - (query - binlog->buffer) - binlog->checksum_size;

            // Fill in event structure, return it too
            event = g_new0(EVENT, 1);
            event->log_file = strdup(binlog->filename);
            event->log_position = binlog->position;
            event->database = strndup(db, qeh->db_length);
            event->timestamp = eh.timestamp;

            // We use binary-compatible query storage
            event->query = malloc(query_length);
            event->query_length = query_length;
            memcpy(event->query, query, query_length);
            if (binlog->insert_id)
                event->insert_id = binlog->insert_id;
            if (binlog->last_insert_id)
                event->last_insert_id = binlog->last_insert_id;

            binlog->position += eh.event_length;
            return event;
        case INTVAR_EVENT:
            iv = binlog->buffer;
            switch (iv->type) {
            case 1:
                binlog->insert_id = iv->value;
                break;
            case 2:
                binlog->last_insert_id = iv->value;
                break;
            }
            break;
        }
        binlog->position += eh.event_length;
    }
}

void seek_binlog(BINLOG * binlog, guint32 position)
{
    fseek(binlog->fd, position, SEEK_SET);
    binlog->position = position;
    binlog->insert_id = binlog->last_insert_id = 0;
}

void free_event(EVENT * event)
{
    free(event->log_file);
    free(event->database);
    free(event->query);
    g_free(event);
}

void free_binlog(BINLOG * binlog)
{
    g_free(binlog->filename);
    free(binlog->buffer);
    free(binlog->vbuffer);
    fclose(binlog->fd);
    g_free(binlog);
}
