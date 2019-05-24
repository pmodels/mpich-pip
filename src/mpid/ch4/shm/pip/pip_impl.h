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
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_enqueue)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t * task)
{
    // int err = 0;
    int err;

    task->next = NULL;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    if (task_queue->tail) {
        task_queue->tail->next = task;
        task_queue->tail = task;
    } else {
        task_queue->head = task_queue->tail = task;
    }
    // task_queue->task_num++;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    // if (err)
    //     printf("MPIDI_PIP_Task_safe_enqueue lock error %d\n", err);

    // MPIDI_PIP_task_t *old_tail =
    //     (MPIDI_PIP_task_t *) __sync_lock_test_and_set(&task_queue->tail, task);
    // old_tail->next = task;


    // printf("rank %d - after enqueue, task_queue %p has task num %d\n", pip_global.local_rank, task_queue,
    //        task_queue->task_num);
    // fflush(stdout);
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_dequeue)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t ** task)
{
    int err;

    // if (err)
    //     printf("MPIDI_PIP_Task_safe_dequeue lock error %d\n", err);
    MPIDI_PIP_task_t *old_head;
    // if (old_head) {
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    old_head = task_queue->head;
    if (old_head) {
        task_queue->head = old_head->next;
        if (task_queue->head == NULL)
            task_queue->tail = NULL;
        // task_queue->task_num--;
    }
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    // }

    *task = old_head;

    // printf("rank %d - after dequeue, task_queue %p has task num %d\n", pip_global.local_rank, task_queue,
    //        task_queue->task_num);
    // fflush(stdout);

    // MPIDI_PIP_task_t *old_head;
    // MPIDI_PIP_task_t *old_tail;
    // MPIDI_PIP_task_t *new_head;
    // do {
    //   retry:
    //     old_head = task_queue->head->next;
    //     if (old_head == NULL || task_queue->tail == task_queue->head) {
    //         old_head = NULL;
    //         break;
    //     }
    //     new_head = old_head->next;
    //     if (new_head == NULL) {
    //         if (!__sync_bool_compare_and_swap(&task_queue->tail, old_head, task_queue->head)) {
    //             /* unsuccessful, others have inserted a new task or has been reset to head by another process */
    //             goto retry;
    //         } else {
    //             /* set tail to dummy task */
    //             __sync_bool_compare_and_swap(&task_queue->head->next, old_head, NULL);
    //             break;
    //         }
    //     }
    // } while (!__sync_bool_compare_and_swap(&task_queue->head->next, old_head, new_head));

    // *task = old_head;
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Compl_task_safe_enqueue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t * task)
{
    // int err = 0;
    int mpi_errno = MPI_SUCCESS, err;

    task->next = NULL;
    if (task_queue->tail) {
        task_queue->tail->next = task;
        task_queue->tail = task;
    } else {
        task_queue->head = task_queue->tail = task;
    }
    // MPID_Thread_mutex_lock(&task_queue->lock, &err);
    // task_queue->task_num++;
    // *task->cur_task_id = task->task_id + 1;

    // MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    // if (err)
    //     printf("MPIDI_PIP_Task_safe_enqueue lock error %d\n", err);

    // MPIDI_PIP_task_t *old_tail =
    //     (MPIDI_PIP_task_t *) __sync_lock_test_and_set(&task_queue->tail, task);
    // old_tail->next = task;


    // printf("rank %d - after enqueue, task_queue %p has task num %d\n", pip_global.local_rank, task_queue,
    //        task_queue->task_num);
    // fflush(stdout);
    return mpi_errno;
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Compl_task_safe_dequeue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_delete_head(MPIDI_PIP_task_queue_t * task_queue)
{
    int mpi_errno = MPI_SUCCESS, err;

    // if (err)
    //     printf("MPIDI_PIP_Task_safe_dequeue lock error %d\n", err);
    MPIDI_PIP_task_t *old_head = task_queue->head;
    // if (old_head) {
    // if (old_head) {
    //     if (old_head->next) {
    //         task_queue->head->next = old_head->next;
    //         task_queue->task_num--;
    //     } else {
    if (old_head) {
        task_queue->head = old_head->next;
        if (task_queue->head == NULL)
            task_queue->tail = NULL;
    }
    // MPID_Thread_mutex_lock(&task_queue->lock, &err);
    // old_head = task_queue->head->next;
    // if (old_head) {

    // task_queue->task_num--;
    // }
    // MPID_Thread_mutex_unlock(&task_queue->lock, &err);

    //     }
    // }


    // printf("rank %d - after dequeue, task_queue %p has task num %d\n", pip_global.local_rank, task_queue,
    //        task_queue->task_num);
    // fflush(stdout);

    // MPIDI_PIP_task_t *old_head;
    // MPIDI_PIP_task_t *old_tail;
    // MPIDI_PIP_task_t *new_head;
    // do {
    //   retry:
    //     old_head = task_queue->head->next;
    //     if (old_head == NULL || task_queue->tail == task_queue->head) {
    //         old_head = NULL;
    //         break;
    //     }
    //     new_head = old_head->next;
    //     if (new_head == NULL) {
    //         if (!__sync_bool_compare_and_swap(&task_queue->tail, old_head, task_queue->head)) {
    //             /* unsuccessful, others have inserted a new task or has been reset to head by another process */
    //             goto retry;
    //         } else {
    //             /* set tail to dummy task */
    //             __sync_bool_compare_and_swap(&task_queue->head->next, old_head, NULL);
    //             break;
    //         }
    //     }
    // } while (!__sync_bool_compare_and_swap(&task_queue->head->next, old_head, new_head));

    // *task = old_head;
    return mpi_errno;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_fflush_compl_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_compl_task()
{
    MPIDI_PIP_task_t *task = pip_global.local_compl_queue->head;
    while (task && task->compl_flag) {
        MPIDI_PIP_Compl_task_delete_head(pip_global.local_compl_queue);
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
        task = pip_global.local_compl_queue->head;
        // if (task) {
        //      // if (MPIDI_POSIX_mem_region.local_rank == 1)
        //      // printf("rank %d - complete task %p BEGIN\n", MPIDI_POSIX_mem_region.local_rank, task);
        //      // fflush(stdout);
        //      MPIDI_PIP_do_task_compl(task);
        //      // if (MPIDI_POSIX_mem_region.local_rank == 1)
        //      // printf("rank %d - complete task %p END\n", MPIDI_POSIX_mem_region.local_rank, task);
        //      // fflush(stdout);
        // } else
        //      break;
    }
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_do_task_copy)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_do_task_copy(MPIDI_PIP_task_t * task)
{
    int mpi_errno = MPI_SUCCESS;
    int task_id = task->task_id;
    // void *recv_buffer;
    MPIDI_POSIX_cell_ptr_t cell = task->cell;
    if (task->data_sz) {
        MPIR_Request *req = task->req;
        if (MPIDI_POSIX_REQUEST(req)->segment_ptr) {
            printf("rank %d - dealing with non-contig data right now\n", pip_global.local_rank);
            fflush(stdout);
            // size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + task->data_sz;
            // MPIR_Segment_pack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
            //                   MPIDI_POSIX_REQUEST(req)->segment_first, (MPI_Aint *) & last,
            //                   recv_buffer);
            // MPIDI_POSIX_REQUEST(req)->segment_first = last;
            /* non-contig */
        } else {
            /* contig */
            // printf("rank %d - send data size %ld, task %p, src %p, dest %p\n", pip_global.local_rank,
            //        task->data_sz, task, task->src_first, task->dest);
            // fflush(stdout);
            MPIR_Memcpy(task->dest, task->src_first, task->data_sz);
        }
        // pip_global.copy_size += task->data_sz;
    }
    // task->next = NULL;

    while (task_id != *task->cur_task_id);
    MPIDI_POSIX_queue_enqueue(task->cellQ, cell);
    *task->cur_task_id = task_id + 1;
    OPA_write_barrier();
    task->compl_flag = 1;

    // MPIDI_PIP_Compl_task_safe_enqueue(task->compl_queue, task);
    // *task->cur_task_id = task_id + 1;


    // if (cell->pkt.mpich.type != MPIDI_POSIX_TYPELMT) {
    //     MPIDI_PIP_task_request_complete(task->MPIR_Request_mem, req);
    //     (*task->completion_count)++;
    // }

// if (in_cell) {
//     cell->pending = NULL;
//     // printf("rank %d - receive size %ld, task %p, freeQ %p\n", pip_global.local_rank, task->data_sz, task, task->cellQ);
//     // fflush(stdout);
//     MPIDI_POSIX_queue_enqueue(task->cellQ, cell);
// } else {
//     MPIR_Request *unexp_req = task->unexp_req;
//     // MPIDI_POSIX_REQUEST(unexp_req)->pending = NULL;
//     MPL_free(MPIDI_POSIX_REQUEST(unexp_req)->user_buf);
//     MPIDI_PIP_task_request_complete(task->MPIR_Request_mem, unexp_req);
// }

// if (type == MPIDI_POSIX_TYPEEAGER) {
//     MPIDI_PIP_task_request_complete(task->MPIR_Request_mem, req);
//     (*task->completion_count)++;
// }

// MPIR_Handle_obj_free(task->MPIDI_Task_mem, task);
    return mpi_errno;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_fflush_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_task()
{
    MPIDI_PIP_task_t *task;
    while (pip_global.local_task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(pip_global.local_task_queue, &task);

        /* find my own task */
        MPIDI_PIP_do_task_copy(task);
    }
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_steal_task)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_steal_task()
{
    int victim = rand() % pip_global.num_local;
    MPIDI_PIP_task_t *task = NULL;
    if (victim != pip_global.local_rank) {
#ifdef MPI_PIP_SHM_TASK_STEAL
        if (pip_global.shm_task_queue[victim]->head) {
            MPIDI_PIP_Task_safe_dequeue(pip_global.shm_task_queue[victim], &task);
            MPIDI_PIP_do_task_copy(task);
        }
        // pip_global.try_steal++;
        // pip_global.esteal_try[victim]++;

        // pip_global.esteal_done[victim]++;
        // pip_global.suc_steal++;
        // printf
        //     ("Process %d steal task from victim %d, task %p, data size %ld, remaining task# %d, queue %p\n",
        //      pip_global.local_rank, victim, task, task->data_sz,
        //      pip_global.shm_task_queue[victim]->task_num, pip_global.shm_task_queue[victim]);
        // fflush(stdout);


#endif

#ifdef MPI_PIP_NM_TASK_STEAL

#endif
    }
}
#endif
