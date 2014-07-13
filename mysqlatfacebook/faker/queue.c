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

#include <glib.h>

typedef struct {
    GQueue *queue;
    GMutex *mutex;
    GCond *read_cond;
    GCond *write_cond;
    guint32 max;
} SizedQueue;

void *sq_init(int max)
{

    SizedQueue *sq = g_new0(SizedQueue, 1);

    sq->queue = g_queue_new();
    sq->mutex = g_mutex_new();
    sq->read_cond = g_cond_new();
    sq->write_cond = g_cond_new();
    sq->max = max;

    return sq;
}

gpointer sq_pop(SizedQueue * sq)
{
    g_mutex_lock(sq->mutex);
    while (g_queue_is_empty(sq->queue))
        g_cond_wait(sq->read_cond, sq->mutex);

    gpointer ret = g_queue_pop_tail(sq->queue);

    if (g_queue_get_length(sq->queue) < sq->max)
        g_cond_signal(sq->write_cond);
    g_mutex_unlock(sq->mutex);
    return ret;
}

void sq_push(SizedQueue * sq, gpointer data)
{
    g_mutex_lock(sq->mutex);
    while (g_queue_get_length(sq->queue) >= sq->max)
        g_cond_wait(sq->write_cond, sq->mutex);
    g_queue_push_head(sq->queue, data);
    g_cond_signal(sq->read_cond);
    g_mutex_unlock(sq->mutex);
}
