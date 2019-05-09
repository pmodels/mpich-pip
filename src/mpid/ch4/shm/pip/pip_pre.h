/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2006 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 *
 *  Portions of this code were written by Intel Corporation.
 *  Copyright (C) 2011-2016 Intel Corporation.  Intel provides this material
 *  to Argonne National Laboratory subject to Software Grant and Corporate
 *  Contributor License Agreement dated February 8, 2012.
 */

#ifndef PIP_PRE_H_INCLUDED
#define PIP_PRE_H_INCLUDED
#include <mpidimpl.h>
#include <../posix/posix_datatypes.h>

#define MPIDI_TASK_PREALLOC 64

struct MPIDI_PIP_task_queue;

/* Use cell->pkt.mpich.type = MPIDI_POSIX_TYPEEAGER to judge the complete transfer */
typedef struct MPIDI_PIP_task {
    MPIR_OBJECT_HEADER;
    MPIR_Request *req;
    union {
        MPIDI_POSIX_cell_ptr_t cell;
        MPIR_Request *unexp_req;
    };

    volatile uint64_t *cur_task_id;
    uint64_t task_id;
    uint64_t in_cell;
    int type;
    int send_flag;
    int *completion_count;
    MPIDI_POSIX_queue_ptr_t cellQ;
    struct MPIDI_PIP_task_queue *compl_queue;
    void *src_first;
    void *dest;
    size_t data_sz;
    struct MPIDI_PIP_task *next;
} MPIDI_PIP_task_t;

typedef struct MPIDI_PIP_task_queue {
    MPIDI_PIP_task_t *head;
    MPIDI_PIP_task_t *tail;
    // MPID_Thread_mutex_t lock;
    // int task_num;
} MPIDI_PIP_task_queue_t;

typedef struct {
    uint32_t num_local;
    uint32_t local_rank;
    uint64_t *local_send_counter;
    uint64_t *shm_send_counter;
    uint64_t *local_recv_counter;
    uint64_t *shm_recv_counter;
    MPIDI_PIP_task_queue_t *local_task_queue;
    MPIDI_PIP_task_queue_t **shm_task_queue;
    MPIDI_PIP_task_queue_t *local_compl_queue;
} MPIDI_PIP_global_t;

extern MPIDI_PIP_global_t pip_global;

#endif /* PIP_PRE_H_INCLUDED */
