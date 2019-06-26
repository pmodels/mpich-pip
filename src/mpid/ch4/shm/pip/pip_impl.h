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

extern MPIR_Object_alloc_t MPIDI_Segment_mem;

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_enqueue)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t * task)
{
    // int err = 0;
    int err;

    // task->next = NULL;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    if (task_queue->tail) {
        task_queue->tail->next = task;
        task_queue->tail = task;
    } else {
        task_queue->head = task_queue->tail = task;
    }
    task_queue->task_num++;
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Task_safe_dequeue)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_Task_safe_dequeue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t ** task)
{
    int err;

    MPIDI_PIP_task_t *old_head;
    MPID_Thread_mutex_lock(&task_queue->lock, &err);
    old_head = task_queue->head;
    if (old_head) {
        task_queue->head = old_head->next;
        if (task_queue->head == NULL)
            task_queue->tail = NULL;
        task_queue->task_num--;
    }
    MPID_Thread_mutex_unlock(&task_queue->lock, &err);
    *task = old_head;

    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Compl_task_safe_enqueue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_enqueue(MPIDI_PIP_task_queue_t * task_queue,
                                                          MPIDI_PIP_task_t * task)
{
    int mpi_errno = MPI_SUCCESS, err;

    if (task_queue->tail) {
        task_queue->tail->compl_next = task;
        task_queue->tail = task;
    } else {
        task_queue->head = task_queue->tail = task;
    }

    task_queue->task_num++;

    return mpi_errno;
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_Compl_task_safe_dequeue)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_Compl_task_delete_head(MPIDI_PIP_task_queue_t * task_queue)
{
    int mpi_errno = MPI_SUCCESS, err;

    MPIDI_PIP_task_t *old_head = task_queue->head;
    if (old_head) {
        task_queue->head = old_head->compl_next;
        if (task_queue->head == NULL)
            task_queue->tail = NULL;
        task_queue->task_num--;
    }
    return mpi_errno;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_fflush_compl_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_compl_task(MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task = compl_queue->head;
    while (task && task->compl_flag) {
        MPIDI_PIP_Compl_task_delete_head(compl_queue);
        if (task->segp)
            MPIR_Handle_obj_free(&MPIDI_Segment_mem, task->segp);
        if (task->unexp_req) {
            MPIR_Request *sreq = task->unexp_req;
            MPIDI_POSIX_REQUEST(sreq)->pending = NULL;
            MPL_free(MPIDI_POSIX_REQUEST(sreq)->user_buf);
            MPIDI_POSIX_REQUEST_COMPLETE(sreq);
        }
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
        task = compl_queue->head;
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


    // struct timespec start, end;
    // clock_gettime(CLOCK_MONOTONIC, &start);
    if (task->segp) {
        DLOOP_Segment *segp = task->segp;
        size_t last = task->segment_first + task->data_sz;
        if (task->send_flag) {
            MPIR_Segment_pack(segp, task->segment_first, (MPI_Aint *) & last, task->dest);
        } else {
            MPIR_Segment_unpack(segp, task->segment_first, (MPI_Aint *) & last, task->src);
        }
        /* non-contig */
    } else {
        /* contig */
        MPIR_Memcpy(task->dest, task->src, task->data_sz);
    }
    pip_global.copy_size += task->data_sz;

    // task->next = NULL;
    MPIDI_POSIX_cell_ptr_t cell = task->cell;
    if (task->send_flag) {
        while (task_id != *task->cur_task_id);
        MPIDI_POSIX_PIP_queue_enqueue(task->cell_queue, cell, task->asym_addr);
        *task->cur_task_id = task_id + 1;
        OPA_write_barrier();
    } else {
        if (cell)
            MPIDI_POSIX_PIP_queue_enqueue(task->cell_queue, cell, task->asym_addr);
    }

    // *task->cur_task_id = task_id + 1;
    // OPA_write_barrier();
    // clock_gettime(CLOCK_MONOTONIC, &end);
    // double time = (end.tv_sec - start.tv_sec) * 1e6 + (end.tv_nsec - start.tv_nsec) / 1e3;
    // printf("rank %d - copy data from %d, size %ld, time %.3lfus\n", pip_global.local_rank, task->rank,
    //        task->data_sz, time);
    // fflush(stdout);
    task->compl_flag = 1;

    return mpi_errno;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_compl_one_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_compl_one_task(MPIDI_PIP_task_queue_t * compl_queue)
{
    MPIDI_PIP_task_t *task = compl_queue->head;
    if (task && task->compl_flag) {
        MPIDI_PIP_Compl_task_delete_head(compl_queue);
        if (task->segp)
            MPIR_Handle_obj_free(&MPIDI_Segment_mem, task->segp);
        if (task->unexp_req) {
            MPIR_Request *sreq = task->unexp_req;
            MPIDI_POSIX_REQUEST(sreq)->pending = NULL;
            MPL_free(MPIDI_POSIX_REQUEST(sreq)->user_buf);
            MPIDI_POSIX_REQUEST_COMPLETE(sreq);
        }
        MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
    }
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_fflush_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_exec_task(MPIDI_PIP_task_queue_t * task_queue)
{
    MPIDI_PIP_task_t *task;

    if (task_queue->head) {
        MPIDI_PIP_Task_safe_dequeue(task_queue, &task);
        /* find my own task */
        if (task) {
            MPIDI_PIP_do_task_copy(task);
            MPIDI_PIP_compl_one_task(pip_global.local_compl_queue);
        }
    }
    return;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_fflush_task)
MPL_STATIC_INLINE_PREFIX void MPIDI_PIP_fflush_task()
{
    MPIDI_PIP_task_t *task;
    int i;
    for (i = 0; i < pip_global.numa_max_node; ++i) {
        while (pip_global.task_queue[i].head) {
            MPIDI_PIP_Task_safe_dequeue(&pip_global.task_queue[i], &task);
            /* find my own task */
            if (task) {
                MPIDI_PIP_do_task_copy(task);
            }
        }
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
        MPIDI_PIP_task_queue_t *victim_queue =
            &pip_global.shm_task_queue[victim][pip_global.local_numa_id];
        if (victim_queue->head) {
            // __sync_add_and_fetch(&pip_global.shm_in_proc[victim], 1);
            MPIDI_PIP_Task_safe_dequeue(victim_queue, &task);
            // __sync_sub_and_fetch(&pip_global.shm_in_proc[victim], 1);
            if (task)
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
