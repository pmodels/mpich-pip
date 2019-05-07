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

#ifndef PIP_IMPL_H_INCLUDED
#define PIP_IMPL_H_INCLUDED

#include "pip_pre.h"

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_do_task_copy)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_do_task_copy(MPIDI_PIP_task_t * task, int *completion_count)
{
    int mpi_errno = MPI_SUCCESS;
    int cell_id = task->cell_id;
    MPIDI_POSIX_cell_ptr_t cell = task->cell;
    MPIR_Request *req = task->req;
    void *recv_buffer;

    if (task->send_flag) {
        /* send task */
        if (cell->pkt.mpich.type != MPIDI_POSIX_TYPEACK && task->data_sz) {
            if (MPIDI_POSIX_REQUEST(req)->segment_ptr) {
                printf("rank %d - dealing with non-contig data right now\n", pip_global.local_rank);
                fflush(stdout);
                size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + task->data_sz;
                MPIR_Segment_pack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
                                  MPIDI_POSIX_REQUEST(req)->segment_first, (MPI_Aint *) & last,
                                  recv_buffer);
                MPIDI_POSIX_REQUEST(req)->segment_first = last;
                /* non-contig */
            } else {
                /* contig */
                // printf("rank %d - send data size %ld, task %p\n", pip_global.local_rank,
                //        task->data_sz, task);
                // fflush(stdout);
                MPIR_Memcpy(task->dest, task->src_first, task->data_sz);
            }
        }

        while (cell_id != *task->cur_cell_id);
        MPIDI_POSIX_queue_enqueue(task->cellQ, cell);
        *task->cur_cell_id = cell_id + 1;

        if (cell->pkt.mpich.type != MPIDI_POSIX_TYPELMT) {
            MPIDI_POSIX_REQUEST_COMPLETE(req);
            (*completion_count)++;
        }

    } else {
        /* receive task */
        // printf("Not implemented receiver side task copy\n");
        // fflush(stdout);
        long in_cell = task->in_cell;
        long type = task->type;

        if (task->data_sz) {
            if (MPIDI_POSIX_REQUEST(req)->segment_ptr) {
                printf("rank %d - dealing with receive non-contig data right now\n",
                       pip_global.local_rank);
                fflush(stdout);
                size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + task->data_sz;
                MPIR_Segment_pack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
                                  MPIDI_POSIX_REQUEST(req)->segment_first, (MPI_Aint *) & last,
                                  recv_buffer);
                MPIDI_POSIX_REQUEST(req)->segment_first = last;
                /* non-contig */
            } else {
                /* contig */
                MPIR_Memcpy(task->dest, task->src_first, task->data_sz);
            }
        }

        if (in_cell) {
            cell->pending = NULL;
            // printf("rank %d - receive size %ld, task %p, freeQ %p\n", pip_global.local_rank, task->data_sz, task, task->cellQ);
            // fflush(stdout);
            MPIDI_POSIX_queue_enqueue(task->cellQ, cell);
        } else {
            MPIR_Request *unexp_req = task->unexp_req;
            // MPIDI_POSIX_REQUEST(unexp_req)->pending = NULL;
            MPL_free(MPIDI_POSIX_REQUEST(unexp_req)->user_buf);
            MPIDI_POSIX_REQUEST_COMPLETE(unexp_req);
        }

        if (type == MPIDI_POSIX_TYPEEAGER) {
            // if (in_cell) {
            //      cell->pending = NULL;
            //      printf("rank %d - receive size %ld, task %p, freeQ %p\n", pip_global.local_rank, task->data_sz, task, task->cellQ);
            //      fflush(stdout);
            //      MPIDI_POSIX_queue_enqueue(task->cellQ, cell);
            // } else {
            //      MPIR_Request *unexp_req = task->unexp_req;
            //      MPIDI_POSIX_REQUEST(unexp_req)->pending = NULL;
            //      MPL_free(MPIDI_POSIX_REQUEST(unexp_req)->user_buf);
            //      MPIDI_POSIX_REQUEST_COMPLETE(unexp_req);
            // }
            MPIDI_POSIX_REQUEST_COMPLETE(req);
            (*completion_count)++;
        }
        // else{

        // }
    }

    MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    return mpi_errno;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_enqueue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                         MPIDI_PIP_task_t * task)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_PIP_task_t *old_tail =
        (MPIDI_PIP_task_t *) __sync_lock_test_and_set(&task_queue->tail, task);
    old_tail->next = task;
    return mpi_errno;
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_dequeue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_task_queue_t * task_queue,
                                                         MPIDI_PIP_task_t ** task)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDI_PIP_task_t *old_head;
    MPIDI_PIP_task_t *old_tail;
    MPIDI_PIP_task_t *new_head;
    do {
      retry:
        old_head = task_queue->head->next;
        if (old_head == NULL || task_queue->tail == task_queue->head)
            break;
        new_head = old_head->next;
        if (new_head == NULL) {
            if (!__sync_bool_compare_and_swap(&task_queue->tail, old_head, task_queue->head)) {
                /* unsuccessful, others have inserted a new task */
                goto retry;
            } else {
                /* set tail to dummy task */
                __sync_bool_compare_and_swap(&task_queue->head->next, old_head, NULL);
                break;
            }
        }
    } while (!__sync_bool_compare_and_swap(&task_queue->head->next, old_head, new_head));

    *task = old_head;
    return mpi_errno;
}

// #undef FCNAME
// #define FCNAME MPL_QUOTE(MPIDI_PIP_steal_task)
// MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_steal_task()
// {
//     int victim = rand() % pip_global.num_local;
//     if (victim != pip_global.local_rank) {

//     }
// }
#endif
