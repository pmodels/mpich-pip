/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2016 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 *
 *  Portions of this code were written by Mellanox Technologies Ltd.
 *  Copyright (C) Mellanox Technologies Ltd. 2016. ALL RIGHTS RESERVED
 */
#ifndef UCX_DATATYPE_H_INCLUDED
#define UCX_DATATYPE_H_INCLUDED

#include "ucx_impl.h"
#include "ucx_types.h"
#include <ucp/api/ucp.h>
#ifdef HAVE_LIBHCOLL
#include "../../../common/hcoll/hcoll.h"
#endif

struct MPIDI_UCX_pack_state {
    MPIR_Segment *segment_ptr;
    MPI_Aint packsize;
};

static inline void *MPIDI_UCX_Start_pack(void *context, const void *buffer, size_t count)
{
    MPI_Datatype *datatype = (MPI_Datatype *) context;
    MPIR_Segment *segment_ptr;
    struct MPIDI_UCX_pack_state *state;
    MPI_Aint packsize;

    state = MPL_malloc(sizeof(struct MPIDI_UCX_pack_state), MPL_MEM_DATATYPE);
    segment_ptr = MPIR_Segment_alloc();
    MPIR_Pack_size_impl(count, *datatype, &packsize);
    /* Todo: Add error handling */
    MPIR_Segment_init(buffer, count, *datatype, segment_ptr);
    state->packsize = packsize;
    state->segment_ptr = segment_ptr;

    return (void *) state;
}

static inline void *MPIDI_UCX_Start_unpack(void *context, void *buffer, size_t count)
{
    MPI_Datatype *datatype = (MPI_Datatype *) context;
    MPIR_Segment *segment_ptr;
    struct MPIDI_UCX_pack_state *state;
    MPI_Aint packsize;

    state = MPL_malloc(sizeof(struct MPIDI_UCX_pack_state), MPL_MEM_DATATYPE);
    MPIR_Pack_size_impl(count, *datatype, &packsize);
    segment_ptr = MPIR_Segment_alloc();
    /* Todo: Add error handling */
    MPIR_Segment_init(buffer, count, *datatype, segment_ptr);
    state->packsize = packsize;
    state->segment_ptr = segment_ptr;

    return (void *) state;
}

extern MPIR_Object_alloc_t MPIDI_Task_mem;
extern MPIR_Object_alloc_t MPIDI_Segment_mem;
#include "../../shm/pip/pip_pre.h"
#include "../../shm/pip/pip_impl.h"

static inline size_t MPIDI_UCX_Packed_size(void *state)
{
    struct MPIDI_UCX_pack_state *pack_state = (struct MPIDI_UCX_pack_state *) state;

    return (size_t) pack_state->packsize;
}

static inline size_t MPIDI_UCX_Pack(void *state, size_t offset, void *dest, size_t max_length)
{
    struct MPIDI_UCX_pack_state *pack_state = (struct MPIDI_UCX_pack_state *) state;
    MPI_Aint last = MPL_MIN(pack_state->packsize, offset + max_length);

    if(last != pack_state->packsize){
        /* Not last data seg, I can push to queue for stealing */
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
        task->send_flag = 1;
        task->compl_flag = 0;
        task->last = last;
        task->unexp_req = NULL;
        task->next = NULL;
        task->compl_next = NULL;
        task->src = NULL;
        task->dest = dest;
        task->segp = (DLOOP_Segment *) MPIR_Handle_obj_alloc(&MPIDI_Segment_mem);
        MPIR_Memcpy(task->segp, pack_state->segment_ptr, sizeof(DLOOP_Segment));
        task->segment_first = offset;
        MPIR_Segment_manipulate(pack_state->segment_ptr, offset, &last, NULL,      /* contig fn */
                                            NULL,       /* vector fn */
                                            NULL,       /* blkidx fn */
                                            NULL,       /* index fn */
                                            NULL, NULL);
        MPIDI_PIP_Task_safe_enqueue(pip_global.ucx_task_queue, task);
        MPIDI_PIP_Compl_task_enqueue(pip_global.ucx_local_compl_queue, task);
        if (pip_global.ucx_local_compl_queue->task_num >= MPIDI_MAX_TASK_THREASHOLD)
        {
            MPIDI_PIP_fflush_compl_task(pip_global.ucx_local_compl_queue);
        }
    }else{
        /* last one I must fflush all previous data segs */
        // printf("rank %d - SEND LAST offset %ld, max_length %ld, packsize %ld, dt_ptr %p, task_num %d, pack_state->segment_ptr %p\n", pip_global.rank, offset, max_length, pack_state->packsize, dt_ptr, pip_global.ucx_local_compl_queue->task_num, pack_state->segment_ptr);
        // fflush(stdout);
        MPIR_Segment_pack(pack_state->segment_ptr, offset, &last, dest);
        MPIDI_PIP_ucx_fflush_task(pip_global.ucx_task_queue);
        while(pip_global.ucx_local_compl_queue->head)
            MPIDI_PIP_fflush_compl_task(pip_global.ucx_local_compl_queue);   
    }

    return (size_t) last - offset;
}

static inline ucs_status_t MPIDI_UCX_Unpack(void *state, size_t offset, const void *src,
                                            size_t count)
{
    struct MPIDI_UCX_pack_state *pack_state = (struct MPIDI_UCX_pack_state *) state;
    MPI_Aint last = MPL_MIN(pack_state->packsize, offset + count);
    MPI_Aint last_pack = last;

    if(offset == 0){
        /* first seg, need to unpack myself */
        MPIR_Segment_unpack(pack_state->segment_ptr, offset, &last, (void *) src);   
    }else if(last == pack_state->packsize){
        /* last received seg */
        // printf("rank %d - RECV LAST offset %ld, count %ld, packsize %ld, dt_ptr %p, task_num %d, pack_state->segment_ptr %p\n", pip_global.rank, offset, count, pack_state->packsize, dt_ptr, pip_global.ucx_local_compl_queue->task_num, pack_state->segment_ptr);
        // fflush(stdout);
        MPIR_Segment_unpack(pack_state->segment_ptr, offset, &last, (void *) src);
        MPIDI_PIP_ucx_fflush_task(pip_global.ucx_task_queue);
        while(pip_global.ucx_local_compl_queue->head)
            MPIDI_PIP_fflush_compl_task(pip_global.ucx_local_compl_queue);
    }else{
        /* enqueue task */
        MPIDI_PIP_task_t *task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);;
        task->send_flag = 0;
        task->compl_flag = 0;
        task->last = last;
        task->unexp_req = NULL;
        task->next = NULL;
        task->compl_next = NULL;

        // task->task_id = pip_global.local_recv_counter[src_local]++;
        // task->cur_task_id = pip_global.shm_recv_counter + src_local;
        // task->completion_count = completion_count;

        task->src = src;
        task->dest = NULL;
        task->segp =
        (DLOOP_Segment *) MPIR_Handle_obj_alloc(&MPIDI_Segment_mem);
        MPIR_Memcpy(task->segp, pack_state->segment_ptr,
        sizeof(DLOOP_Segment));
        task->segment_first = offset;
        MPIDI_PIP_Task_safe_enqueue(pip_global.ucx_task_queue, task);
        MPIDI_PIP_Compl_task_enqueue(pip_global.ucx_local_compl_queue, task);
        if (pip_global.ucx_local_compl_queue->task_num >= MPIDI_MAX_TASK_THREASHOLD)
        {
            MPIDI_PIP_fflush_compl_task(pip_global.ucx_local_compl_queue);
        }
    }
    if (unlikely(last != last_pack))
        return UCS_ERR_MESSAGE_TRUNCATED;
    return UCS_OK;
}

static inline void MPIDI_UCX_Finish_pack(void *state)
{
    MPIR_Datatype *dt_ptr;
    struct MPIDI_UCX_pack_state *pack_state = (struct MPIDI_UCX_pack_state *) state;
    MPIR_Datatype_get_ptr(pack_state->segment_ptr->handle, dt_ptr);
    MPIR_Segment_free(pack_state->segment_ptr);
    MPIR_Datatype_ptr_release(dt_ptr);
    MPL_free(pack_state);
}

static inline int MPIDI_NM_mpi_type_free_hook(MPIR_Datatype * datatype_p)
{
    if (datatype_p->is_committed && (int) datatype_p->dev.netmod.ucx.ucp_datatype >= 0) {
        ucp_dt_destroy(datatype_p->dev.netmod.ucx.ucp_datatype);
        datatype_p->dev.netmod.ucx.ucp_datatype = -1;
    }
#if HAVE_LIBHCOLL
    hcoll_type_free_hook(datatype_p);
#endif

    return 0;
}

static inline int MPIDI_NM_mpi_type_commit_hook(MPIR_Datatype * datatype_p)
{
    ucp_datatype_t ucp_datatype;
    ucs_status_t status;
    int is_contig;

    datatype_p->dev.netmod.ucx.ucp_datatype = -1;
    MPIR_Datatype_is_contig(datatype_p->handle, &is_contig);

    if (!is_contig) {

        ucp_generic_dt_ops_t dt_ops = {
            .start_pack = MPIDI_UCX_Start_pack,
            .start_unpack = MPIDI_UCX_Start_unpack,
            .packed_size = MPIDI_UCX_Packed_size,
            .pack = MPIDI_UCX_Pack,
            .unpack = MPIDI_UCX_Unpack,
            .finish = MPIDI_UCX_Finish_pack
        };
        status = ucp_dt_create_generic(&dt_ops, datatype_p, &ucp_datatype);
        MPIR_Assertp(status == UCS_OK);
        datatype_p->dev.netmod.ucx.ucp_datatype = ucp_datatype;

    }
#if HAVE_LIBHCOLL
    hcoll_type_commit_hook(datatype_p);
#endif

    return 0;
}

#endif /* UCX_DATATYPE_H_INCLUDED */
