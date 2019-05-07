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

/* Use cell->pkt.mpich.type = MPIDI_POSIX_TYPEEAGER to judge the complete transfer */
typedef struct MPIDI_PIP_task {
    MPIR_OBJECT_HEADER;
    MPIR_Request *req;
    union {
        MPIDI_POSIX_cell_ptr_t cell;
        MPIR_Request *unexp_req;
    };
    union {
        uint64_t cell_id;
        uint64_t in_cell;
    };
    union {
        volatile uint64_t *cur_cell_id;
        uint64_t type;
    };
    int send_to;
    size_t data_sz;
    MPIDI_POSIX_queue_ptr_t cellQ;
    void *src_first;
    void *dest;
    int send_flag;
    struct MPIDI_PIP_task *volatile next;
} MPIDI_PIP_task_t;

typedef struct {
    MPIDI_PIP_task_t *head;
    MPIDI_PIP_task_t *volatile tail;
} MPIDI_PIP_task_queue_t;

typedef struct {
    uint32_t num_local;
    uint32_t local_rank;
    uint64_t *local_counter;
    uint64_t *shm_counter;
    MPIDI_PIP_task_queue_t *local_task_queue;
    MPIDI_PIP_task_queue_t **shm_task_queue;
} MPIDI_PIP_global_t;

extern MPIDI_PIP_global_t pip_global;

#endif /* PIP_PRE_H_INCLUDED */
