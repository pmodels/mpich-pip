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
#ifndef POSIX_INIT_H_INCLUDED
#define POSIX_INIT_H_INCLUDED

#include "posix_impl.h"
#include "ch4_types.h"
#include "mpidu_shm.h"
#include <time.h>
#include <../pip/pip_pre.h>
extern MPIR_Object_alloc_t MPIDI_Task_mem;
/* ------------------------------------------------------- */
/* from mpid/ch3/channels/nemesis/src/mpid_nem_init.c */
/* ------------------------------------------------------- */
extern MPIDI_POSIX_mem_region_t MPIDI_POSIX_mem_region;
extern char *MPIDI_POSIX_asym_base_addr;
extern MPIDI_PIP_global_t pip_global;

void MPIDI_PIP_init()
{
    int mpi_errno = MPI_SUCCESS, i, err;
    int num_local, local_rank;
    uint64_t *task_queue_addr;
    MPIR_Errflag_t errflag = MPIR_ERR_NONE;
    // MPIDI_PIP_task_t *task_dummy, *compl_dummy;
    MPIR_CHKPMEM_DECL(11);

    pip_global.num_local = num_local = MPIDI_POSIX_mem_region.num_local;
    pip_global.local_rank = local_rank = MPIDI_POSIX_mem_region.local_rank;
    MPIR_CHKPMEM_MALLOC(pip_global.local_send_counter, uint64_t *,
                        num_local * sizeof(uint64_t), mpi_errno, "pip_local_send_counter",
                        MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(pip_global.shm_send_counter, uint64_t *, num_local * sizeof(uint64_t),
                        mpi_errno, "pip_shm_send_counter", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(pip_global.local_recv_counter, uint64_t *, num_local * sizeof(uint64_t),
                        mpi_errno, "pip_local_recv_counter", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(pip_global.shm_recv_counter, uint64_t *, num_local * sizeof(uint64_t),
                        mpi_errno, "pip_shm_recv_counter", MPL_MEM_SHM);

    memset(pip_global.local_send_counter, 0, num_local * sizeof(uint64_t));
    memset(pip_global.shm_send_counter, 0, num_local * sizeof(uint64_t));
    memset(pip_global.local_recv_counter, 0, num_local * sizeof(uint64_t));
    memset(pip_global.shm_recv_counter, 0, num_local * sizeof(uint64_t));

    MPIR_CHKPMEM_MALLOC(pip_global.local_task_queue, MPIDI_PIP_task_queue_t *,
                        sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "local_task_queue", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(pip_global.shm_task_queue, MPIDI_PIP_task_queue_t **,
                        num_local * sizeof(MPIDI_PIP_task_queue_t *), mpi_errno, "shm_task_queue",
                        MPL_MEM_SHM);

    pip_global.copy_size = 0;
    pip_global.try_steal = 0;
    pip_global.suc_steal = 0;
    // pip_global.task_num = 0;

    MPIR_CHKPMEM_MALLOC(pip_global.esteal_done, int *,
                        sizeof(int) * num_local, mpi_errno, "esteal_done", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(pip_global.esteal_try, int *,
                        sizeof(int) * num_local, mpi_errno, "esteal_try", MPL_MEM_SHM);

    memset(pip_global.esteal_done, 0, num_local * sizeof(int));
    memset(pip_global.esteal_try, 0, num_local * sizeof(int));

    MPID_Thread_mutex_create(&pip_global.local_task_queue->lock, &err);
    pip_global.local_task_queue->task_num = 0;
    pip_global.local_task_queue->head = pip_global.local_task_queue->tail = NULL;
    // task_dummy = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
    // task_dummy->next = NULL;

    MPIR_CHKPMEM_MALLOC(pip_global.local_compl_queue, MPIDI_PIP_task_queue_t *,
                        sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "local_recv_compl_queue",
                        MPL_MEM_SHM);
    MPID_Thread_mutex_create(&pip_global.local_compl_queue->lock, &err);
    pip_global.local_compl_queue->task_num = 0;
    pip_global.local_compl_queue->head = pip_global.local_compl_queue->tail = NULL;

    if (pip_global.local_rank == 0) {
        MPIR_CHKPMEM_MALLOC(pip_global.shm_in_proc, uint64_t *,
                            pip_global.num_local * sizeof(uint64_t), mpi_errno, "shm_in_proc",
                            MPL_MEM_SHM);
        memset(pip_global.shm_in_proc, 0, pip_global.num_local * sizeof(uint64_t));
    }
    // MPIR_CHKPMEM_MALLOC(pip_global.local_recv_compl_queue, MPIDI_PIP_task_queue_t *,
    //                     sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "local_recv_compl_queue",
    //                     MPL_MEM_SHM);
    // MPID_Thread_mutex_create(&pip_global.local_recv_compl_queue->lock, &err);
    // pip_global.local_recv_compl_queue->task_num = 0;
    // pip_global.local_recv_compl_queue->head = pip_global.local_recv_compl_queue->tail = NULL;


    // MPIR_CHKPMEM_MALLOC(pip_global.local_send_compl_queue, MPIDI_PIP_task_queue_t *,
    //                     sizeof(MPIDI_PIP_task_queue_t), mpi_errno, "local_send_compl_queue",
    //                     MPL_MEM_SHM);
    // MPID_Thread_mutex_create(&pip_global.local_send_compl_queue->lock, &err);
    // pip_global.local_send_compl_queue->task_num = 0;
    // pip_global.local_send_compl_queue->head = pip_global.local_send_compl_queue->tail = NULL;
    // compl_dummy = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
    // compl_dummy->next = NULL;

    // printf("rank %d - I am here node_comm %p\n", local_rank, MPIR_Process.comm_world->node_comm);
    // fflush(stdout);
    if (MPIR_Process.comm_world->node_comm) {
        MPIDU_shm_seg_t pip_memory;
        MPIDU_shm_barrier_t *pip_barrier;
        uint64_t *shm_in_proc_addr;
        mpi_errno =
            MPIDU_shm_seg_alloc(num_local * sizeof(uint64_t),
                                (void **) &task_queue_addr, MPL_MEM_SHM);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        mpi_errno = MPIDU_shm_seg_alloc(sizeof(uint64_t), (void **) &shm_in_proc_addr, MPL_MEM_SHM);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        mpi_errno =
            MPIDU_shm_seg_commit(&pip_memory, &pip_barrier,
                                 num_local, local_rank, MPIDI_POSIX_mem_region.local_procs[0],
                                 MPIDI_POSIX_mem_region.rank, MPL_MEM_SHM);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        task_queue_addr[local_rank] = (uint64_t) pip_global.local_task_queue;
        if (pip_global.local_rank == 0)
            *shm_in_proc_addr = (uint64_t) pip_global.shm_in_proc;
        mpi_errno = MPIDU_shm_barrier(pip_barrier, num_local);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        if (pip_global.local_rank != 0) {
            pip_global.shm_in_proc = (uint64_t *) * shm_in_proc_addr;
        }

        for (i = 0; i < num_local; ++i) {
            // printf("rank %d - get process %d task queue %lx\n", local_rank, i, task_queue_addr[i]);
            // fflush(stdout);
            pip_global.shm_task_queue[i] = (MPIDI_PIP_task_queue_t *) task_queue_addr[i];
        }

        mpi_errno = MPIDU_shm_seg_destroy(&pip_memory, num_local);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);
    } else {
        pip_global.shm_task_queue[0] = pip_global.local_task_queue;
    }

    srand(time(NULL) / (local_rank + 1));

  fn_exit:
    return;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_mpi_init_hook)
static inline int MPIDI_POSIX_mpi_init_hook(int rank, int size, int *n_vnis_provided, int *tag_bits)
{
    int mpi_errno = MPI_SUCCESS;
    int num_local = 0;
    int local_rank = -1;
    int *local_procs = NULL;
    int *local_ranks = NULL;
    int i;
    int grank;
    MPIDI_POSIX_fastbox_t *fastboxes_p = NULL;
    MPIDI_POSIX_cell_t(*cells_p)[MPIDI_POSIX_NUM_CELLS];
    MPIDI_POSIX_queue_t *recv_queues_p = NULL;
    MPIDI_POSIX_queue_t *free_queues_p = NULL;
    MPIDI_av_entry_t *av = NULL;
    MPIR_CHKPMEM_DECL(9);
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_INIT);

    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_INIT);

    *n_vnis_provided = 1;

    MPIDI_POSIX_mem_region.num_seg = 1;
    MPIR_CHKPMEM_MALLOC(MPIDI_POSIX_mem_region.seg, MPIDU_shm_seg_info_t *,
                        MPIDI_POSIX_mem_region.num_seg * sizeof(MPIDU_shm_seg_info_t), mpi_errno,
                        "mem_region segments", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(local_procs, int *, size * sizeof(int), mpi_errno,
                        "local process index array", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(local_ranks, int *, size * sizeof(int), mpi_errno,
                        "mem_region local ranks", MPL_MEM_SHM);

    for (i = 0; i < size; i++) {
        av = MPIDIU_comm_rank_to_av(MPIR_Process.comm_world, i);
        if (MPIDI_av_is_local(av)) {
            if (i == rank) {
                local_rank = num_local;
            }

            local_procs[num_local] = i;
            local_ranks[i] = num_local;
            num_local++;
        }
    }

    MPIDI_POSIX_mem_region.rank = rank;
    MPIDI_POSIX_mem_region.num_local = num_local;
    MPIDI_POSIX_mem_region.num_procs = size;
    MPIDI_POSIX_mem_region.local_procs = local_procs;
    MPIDI_POSIX_mem_region.local_ranks = local_ranks;
    MPIDI_POSIX_mem_region.local_rank = local_rank;
    MPIDI_POSIX_mem_region.next = NULL;

    /* Request fastboxes region */
    mpi_errno =
        MPIDU_shm_seg_alloc(MAX
                            ((num_local * ((num_local - 1) * sizeof(MPIDI_POSIX_fastbox_t))),
                             MPIDI_POSIX_ASYMM_NULL_VAL), (void **) &fastboxes_p, MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Request data cells region */
    mpi_errno =
        MPIDU_shm_seg_alloc(num_local * MPIDI_POSIX_NUM_CELLS * sizeof(MPIDI_POSIX_cell_t),
                            (void **) &cells_p, MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Request free q region */
    mpi_errno =
        MPIDU_shm_seg_alloc(num_local * sizeof(MPIDI_POSIX_queue_t), (void **) &free_queues_p,
                            MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Request recv q region */
    mpi_errno =
        MPIDU_shm_seg_alloc(num_local * sizeof(MPIDI_POSIX_queue_t), (void **) &recv_queues_p,
                            MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Request shared collectives barrier vars region */
    mpi_errno =
        MPIDU_shm_seg_alloc(MPIDI_POSIX_NUM_BARRIER_VARS * sizeof(MPIDI_POSIX_barrier_vars_t),
                            (void **) &MPIDI_POSIX_mem_region.barrier_vars, MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Actually allocate the segment and assign regions to the pointers */
    mpi_errno =
        MPIDU_shm_seg_commit(&MPIDI_POSIX_mem_region.memory, &MPIDI_POSIX_mem_region.barrier,
                             num_local, local_rank, MPIDI_POSIX_mem_region.local_procs[0],
                             MPIDI_POSIX_mem_region.rank, MPL_MEM_SHM);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* post check_alloc steps */
    if (MPIDI_POSIX_mem_region.memory.symmetrical == 1) {
        MPIDI_POSIX_asym_base_addr = NULL;
    } else {
        MPIDI_POSIX_asym_base_addr = MPIDI_POSIX_mem_region.memory.base_addr;
#ifdef MPIDI_POSIX_SYMMETRIC_QUEUES
        MPIR_ERR_INTERNALANDJUMP(mpi_errno, "queues are not symmetrically allocated as expected");
#endif
    }

    /* init shared collectives barrier region */
    mpi_errno = MPIDI_POSIX_barrier_vars_init(MPIDI_POSIX_mem_region.barrier_vars);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* local procs barrier */
    mpi_errno = MPIDU_shm_barrier(MPIDI_POSIX_mem_region.barrier, num_local);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* find our cell region */
    MPIDI_POSIX_mem_region.Elements = cells_p[local_rank];

    /* Tables of pointers to shared memory Qs */
    MPIR_CHKPMEM_MALLOC(MPIDI_POSIX_mem_region.FreeQ, MPIDI_POSIX_queue_ptr_t *,
                        size * sizeof(MPIDI_POSIX_queue_ptr_t), mpi_errno, "FreeQ", MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(MPIDI_POSIX_mem_region.RecvQ, MPIDI_POSIX_queue_ptr_t *,
                        size * sizeof(MPIDI_POSIX_queue_ptr_t), mpi_errno, "RecvQ", MPL_MEM_SHM);

    /* Init table entry for our Qs */
    MPIDI_POSIX_mem_region.FreeQ[rank] = &free_queues_p[local_rank];
    MPIDI_POSIX_mem_region.RecvQ[rank] = &recv_queues_p[local_rank];

    /* Init our queues */
    MPIDI_POSIX_queue_init(MPIDI_POSIX_mem_region.RecvQ[rank]);
    MPIDI_POSIX_queue_init(MPIDI_POSIX_mem_region.FreeQ[rank]);

    /* Init and enqueue our free cells */
    for (i = 0; i < MPIDI_POSIX_NUM_CELLS; ++i) {
        MPIDI_POSIX_cell_init(&(MPIDI_POSIX_mem_region.Elements[i]), rank);
        MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.FreeQ[rank],
                                  &(MPIDI_POSIX_mem_region.Elements[i]));
    }

    /* set route for local procs through shmem */
    for (i = 0; i < num_local; i++) {
        grank = local_procs[i];
        MPIDI_POSIX_mem_region.FreeQ[grank] = &free_queues_p[i];
        MPIDI_POSIX_mem_region.RecvQ[grank] = &recv_queues_p[i];

        MPIR_Assert(MPIDI_POSIX_ALIGNED
                    (MPIDI_POSIX_mem_region.FreeQ[grank], MPIDI_POSIX_CACHE_LINE_LEN));
        MPIR_Assert(MPIDI_POSIX_ALIGNED
                    (MPIDI_POSIX_mem_region.RecvQ[grank], MPIDI_POSIX_CACHE_LINE_LEN));
    }

    /* make pointers to our queues global so we don't have to dereference the array */
    MPIDI_POSIX_mem_region.my_freeQ = MPIDI_POSIX_mem_region.FreeQ[rank];
    MPIDI_POSIX_mem_region.my_recvQ = MPIDI_POSIX_mem_region.RecvQ[rank];

    /* local barrier */
    mpi_errno = MPIDU_shm_barrier(MPIDI_POSIX_mem_region.barrier, num_local);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Allocate table of pointers to fastboxes */
    MPIR_CHKPMEM_MALLOC(MPIDI_POSIX_mem_region.mailboxes.in, MPIDI_POSIX_fastbox_t **,
                        num_local * sizeof(MPIDI_POSIX_fastbox_t *), mpi_errno, "fastboxes",
                        MPL_MEM_SHM);
    MPIR_CHKPMEM_MALLOC(MPIDI_POSIX_mem_region.mailboxes.out, MPIDI_POSIX_fastbox_t **,
                        num_local * sizeof(MPIDI_POSIX_fastbox_t *), mpi_errno, "fastboxes",
                        MPL_MEM_SHM);

    MPIR_Assert(num_local > 0);

#define MPIDI_POSIX_MAILBOX_INDEX(sender, receiver) (((sender) > (receiver)) ? ((num_local-1) * (sender) + (receiver)) :        \
                                          (((sender) < (receiver)) ? ((num_local-1) * (sender) + ((receiver)-1)) : 0))

    /* fill in tables */
    for (i = 0; i < num_local; ++i) {
        if (i == local_rank) {
            /* No fastboxs to myself */
            MPIDI_POSIX_mem_region.mailboxes.in[i] = NULL;
            MPIDI_POSIX_mem_region.mailboxes.out[i] = NULL;
        } else {
            MPIDI_POSIX_mem_region.mailboxes.in[i] =
                &fastboxes_p[MPIDI_POSIX_MAILBOX_INDEX(i, local_rank)];
            MPIDI_POSIX_mem_region.mailboxes.out[i] =
                &fastboxes_p[MPIDI_POSIX_MAILBOX_INDEX(local_rank, i)];
            OPA_store_int(&MPIDI_POSIX_mem_region.mailboxes.in[i]->common.flag.value, 0);
            OPA_store_int(&MPIDI_POSIX_mem_region.mailboxes.out[i]->common.flag.value, 0);
        }
    }

#undef MPIDI_POSIX_MAILBOX_INDEX

    /* There is no restriction on the tag_bits from the posix shmod side */
    *tag_bits = MPIR_TAG_BITS_DEFAULT;

    MPIR_CHKPMEM_COMMIT();

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_INIT);
    return mpi_errno;
  fn_fail:
    /* --BEGIN ERROR HANDLING-- */
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
    /* --END ERROR HANDLING-- */
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_mpi_finalize_hook)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_finalize_hook(void)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_FINALIZE);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_FINALIZE);
    // char results[1024];
    // char buffer[128];
    // sprintf(results, "rank %d - copy size %ld", pip_global.local_rank, pip_global.copy_size);
    // fflush(stdout);
    // int i;
    // for (i = 0; i < pip_global.num_local; i++) {
    //     sprintf(buffer, ", (%d, %d, %d)", i, pip_global.esteal_try[i], pip_global.esteal_done[i]);
    //     strcat(results, buffer);
    // }

    // printf("%s\n", results);
    // fflush(stdout);
    /* local barrier */
    mpi_errno = MPIDU_shm_barrier(MPIDI_POSIX_mem_region.barrier, MPIDI_POSIX_mem_region.num_local);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* from MPIDI_POSIX_init */
    MPL_free(MPIDI_POSIX_mem_region.FreeQ);
    MPL_free(MPIDI_POSIX_mem_region.RecvQ);
    MPL_free(MPIDI_POSIX_mem_region.local_ranks);
    MPL_free(MPIDI_POSIX_mem_region.seg);
    MPL_free(MPIDI_POSIX_mem_region.mailboxes.out);
    MPL_free(MPIDI_POSIX_mem_region.mailboxes.in);
    MPL_free(MPIDI_POSIX_mem_region.local_procs);

    mpi_errno =
        MPIDU_shm_seg_destroy(&MPIDI_POSIX_mem_region.memory, MPIDI_POSIX_mem_region.num_local);

    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_FINALIZE);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_get_vni_attr(int vni)
{
    MPIR_Assert(0 <= vni && vni < 1);
    return MPIDI_VNI_TX | MPIDI_VNI_RX;
}

MPL_STATIC_INLINE_PREFIX void *MPIDI_POSIX_mpi_alloc_mem(size_t size, MPIR_Info * info_ptr)
{
    MPIR_Assert(0);
    return NULL;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_free_mem(void *ptr)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_comm_get_lpid(MPIR_Comm * comm_ptr,
                                                       int idx, int *lpid_ptr, bool is_remote)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_get_node_id(MPIR_Comm * comm, int rank, int *id_p)
{
    *id_p = (int) 0;
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_get_max_node_id(MPIR_Comm * comm, int *max_id_p)
{
    *max_id_p = (int) 0;
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_get_local_upids(MPIR_Comm * comm,
                                                         size_t ** local_upid_size,
                                                         char **local_upids)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_upids_to_lupids(int size,
                                                         size_t * remote_upid_size,
                                                         char *remote_upids, int **remote_lupids)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_create_intercomm_from_lpids(MPIR_Comm * newcomm_ptr,
                                                                     int size, const int lpids[])
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

#undef FUNCNAME
#define FUNCNAME MPIDI_POSIX_mpi_type_commit_hook
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_type_commit_hook(MPIR_Datatype * type)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_SHM_TYPE_CREATE_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_SHM_TYPE_CREATE_HOOK);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_SHM_TYPE_CREATE_HOOK);
    return mpi_errno;
}

#undef FUNCNAME
#define FUNCNAME MPIDI_POSIX_mpi_type_free_hook
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_type_free_hook(MPIR_Datatype * type)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_SHM_TYPE_FREE_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_SHM_TYPE_FREE_HOOK);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_SHM_TYPE_FREE_HOOK);
    return mpi_errno;
}

#undef FUNCNAME
#define FUNCNAME MPIDI_POSIX_mpi_op_commit_hook
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_op_commit_hook(MPIR_Op * op)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_SHM_OP_CREATE_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_SHM_OP_CREATE_HOOK);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_SHM_OP_CREATE_HOOK);
    return mpi_errno;
}

#undef FUNCNAME
#define FUNCNAME MPIDI_POSIX_mpi_op_free_hook
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_mpi_op_free_hook(MPIR_Op * op)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_SHM_OP_FREE_HOOK);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_SHM_OP_FREE_HOOK);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_SHM_OP_FREE_HOOK);
    return mpi_errno;
}

#endif /* POSIX_INIT_H_INCLUDED */
