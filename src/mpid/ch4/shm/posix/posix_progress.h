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
#ifndef POSIX_PROGRESS_H_INCLUDED
#define POSIX_PROGRESS_H_INCLUDED

#include "posix_impl.h"
// #include <../pip/pip_pre.h>
#include <../pip/pip_impl.h>
extern MPIR_Object_alloc_t MPIDI_Task_mem;
extern MPIR_Object_alloc_t MPIDI_Segment_mem;
/* ----------------------------------------------------- */
/* MPIDI_POSIX_progress_recv                     */
/* ----------------------------------------------------- */
#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_progress_recv)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_recv(int blocking, int *completion_count)
{
    int mpi_errno = MPI_SUCCESS;
    size_t data_sz;
    int in_cell = 0;
    MPIDI_PIP_task_t *task = NULL;
    MPIDI_POSIX_cell_ptr_t cell = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_RECV);
    /* try to match with unexpected */
    MPIR_Request *sreq = MPIDI_POSIX_recvq_unexpected.head;
    MPIR_Request *prev_sreq = NULL;
  unexpected_l:

    if (sreq != NULL) {
        int unexp_rank, unexp_tag, unexp_context;
        MPIDI_POSIX_ENVELOPE_GET(MPIDI_POSIX_REQUEST(sreq), unexp_rank, unexp_tag, unexp_context);
        goto match_l;
    }

    /* try to receive from recvq */
    if (MPIDI_POSIX_mem_region.my_recvQ &&
        !MPIDI_POSIX_queue_empty(MPIDI_POSIX_mem_region.my_recvQ)) {
        MPIDI_POSIX_queue_dequeue(MPIDI_POSIX_mem_region.my_recvQ, &cell);
        in_cell = 1;
        goto match_l;
    }

    pip_global.recv_empty = 1;
    goto fn_exit;

  match_l:{
        /* traverse posted receive queue */
        MPIR_Request *req = MPIDI_POSIX_recvq_posted.head;
        MPIR_Request *prev_req = NULL;
        int continue_matching = 1;
        char *send_buffer =
            in_cell ? (char *) cell->pkt.mpich.p.payload : (char *) MPIDI_POSIX_REQUEST(sreq)->
            user_buf;
        int type = in_cell ? cell->pkt.mpich.type : MPIDI_POSIX_REQUEST(sreq)->type;
        MPIR_Request *pending = in_cell ? cell->pending : MPIDI_POSIX_REQUEST(sreq)->pending;

        if (type == MPIDI_POSIX_TYPEACK) {
            /* ACK message doesn't have a matching receive! */
            int c;
            MPIR_Assert(in_cell);
            MPIR_Assert(pending);
            MPIR_cc_decr(pending->cc_ptr, &c);
            MPIR_Request_free(pending);
            (*completion_count)++;
            goto release_cell_l;
        }

        while (req) {
            int sender_rank, tag, context_id;
            MPI_Count count;
            MPIDI_POSIX_ENVELOPE_GET(MPIDI_POSIX_REQUEST(req), sender_rank, tag, context_id);

            MPL_DBG_MSG_FMT(MPIR_DBG_HANDLE, TYPICAL,
                            (MPL_DBG_FDEST, "Posted from grank %d to %d in progress %d,%d,%d\n",
                             MPIDI_CH4U_rank_to_lpid(sender_rank, req->comm),
                             MPIDI_POSIX_mem_region.rank, sender_rank, tag, context_id));

            if ((in_cell && MPIDI_POSIX_ENVELOPE_MATCH(cell, sender_rank, tag, context_id)) ||
                (sreq &&
                 MPIDI_POSIX_ENVELOPE_MATCH(MPIDI_POSIX_REQUEST(sreq), sender_rank, tag,
                                            context_id))) {

                /* Request matched */
                pip_global.recv_empty = 0;
                continue_matching = 1;

                if (MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req)) {
                    MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req))
                        = NULL;
                    MPIDI_CH4R_anysource_matched(MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req),
                                                 MPIDI_CH4R_SHM, &continue_matching);

                    /* The request might have been freed during
                     * MPIDI_CH4R_anysource_matched. Double check that it still
                     * exists. */
                    if (MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req)) {
                        MPIR_Request_free(MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req));

#ifndef MPIDI_CH4_DIRECT_NETMOD
                        /* Decouple requests */
                        MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER
                                                             (req))
                            = NULL;
                        MPIDI_CH4I_REQUEST_ANYSOURCE_PARTNER(req) = NULL;
#endif
                    }

                    if (continue_matching)
                        break;
                }



                if (pending) {
                    /* we must send ACK */
                    int srank = in_cell ? cell->rank : sreq->status.MPI_SOURCE;
                    MPIR_Request *req_ack = NULL;
                    MPIDI_POSIX_REQUEST_CREATE_SREQ(req_ack);
                    MPIR_Object_set_ref(req_ack, 1);
                    req_ack->comm = req->comm;
                    MPIR_Comm_add_ref(req->comm);

                    MPIDI_POSIX_ENVELOPE_SET(MPIDI_POSIX_REQUEST(req_ack), req->comm->rank, tag,
                                             context_id);
                    MPIDI_POSIX_REQUEST(req_ack)->user_buf = NULL;
                    MPIDI_POSIX_REQUEST(req_ack)->user_count = 0;
                    MPIDI_POSIX_REQUEST(req_ack)->datatype = MPI_BYTE;
                    MPIDI_POSIX_REQUEST(req_ack)->data_sz = 0;
                    MPIDI_POSIX_REQUEST(req_ack)->type = MPIDI_POSIX_TYPEACK;
                    MPIDI_POSIX_REQUEST(req_ack)->dest = srank;
                    MPIDI_POSIX_REQUEST(req_ack)->next = NULL;
                    MPIDI_POSIX_REQUEST(req_ack)->segment_ptr = NULL;
                    MPIDI_POSIX_REQUEST(req_ack)->pending = pending;
                    /* enqueue req_ack */
                    MPIDI_POSIX_REQUEST_ENQUEUE(req_ack, MPIDI_POSIX_sendq);
                }

                if (type == MPIDI_POSIX_TYPEEAGER || type == MPIDI_POSIX_TYPELMT ||
                    type == MPIDI_POSIX_TYPELMT_LAST) {
                    /* eager message */
                    data_sz =
                        in_cell ? cell->pkt.mpich.datalen : MPIDI_POSIX_REQUEST(sreq)->data_sz;
                } else {
                    data_sz = 0;        /*  unused warning */
                    MPIR_Assert(0);
                }
                /* check for user buffer overflow */
                size_t user_data_sz = MPIDI_POSIX_REQUEST(req)->data_sz;
                if (user_data_sz < data_sz) {
                    req->status.MPI_ERROR = MPI_ERR_TRUNCATE;
                    data_sz = user_data_sz;
                }
                // int grank = MPIDI_CH4U_rank_to_lpid(sender_rank, req->comm);

                /* copy to user buffer */
                // int src_local = MPIDI_POSIX_mem_region.local_ranks[grank];
                if (MPIDI_POSIX_REQUEST(req)->segment_ptr) {
                    /* non-contig */
                    // printf("rank %d - progress engine receive non-contig data\n",
                    //        MPIDI_POSIX_mem_region.local_rank);
                    // fflush(stdout);
                    pip_global.copy_size += data_sz;
                    /* non-contig */
                    // size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + data_sz;
                    // MPIR_Segment_unpack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
                    //                     MPIDI_POSIX_REQUEST(req)->segment_first,
                    //                     (MPI_Aint *) & last, send_buffer);
                    // if (last != MPIDI_POSIX_REQUEST(req)->segment_first + data_sz)
                    //     req->status.MPI_ERROR = MPI_ERR_TYPE;
                    // if (type == MPIDI_POSIX_TYPEEAGER)
                    //     MPIR_Segment_free(MPIDI_POSIX_REQUEST(req)->segment_ptr);
                    // else
                    //     MPIDI_POSIX_REQUEST(req)->segment_first = last;

                    // printf("rank %d - Receiver get non-contiguous data\n", pip_global.local_rank);
                    // fflush(stdout);
                    size_t first = in_cell ? (uint64_t) cell->addr_offset : (uint64_t)
                        MPIDI_POSIX_REQUEST(sreq)->addr_offset;
                    size_t last = first + data_sz;
                    if (type == MPIDI_POSIX_TYPEEAGER) {
                        // size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + data_sz;
                        MPIR_Segment_unpack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
                                            first, (MPI_Aint *) & last, send_buffer);
                        if (last != MPIDI_POSIX_REQUEST(req)->segment_first + data_sz)
                            req->status.MPI_ERROR = MPI_ERR_TYPE;
                        // if (in_cell)
                        MPIR_Segment_free(MPIDI_POSIX_REQUEST(req)->segment_ptr);
                    } else {
                        // size_t last = MPIDI_POSIX_REQUEST(req)->segment_first + data_sz;
                        if (type == MPIDI_POSIX_TYPELMT_LAST) {
                            MPIR_Segment_unpack(MPIDI_POSIX_REQUEST(req)->segment_ptr,
                                                first, (MPI_Aint *) & last, send_buffer);
                            if (last != MPIDI_POSIX_REQUEST(req)->segment_first + data_sz)
                                req->status.MPI_ERROR = MPI_ERR_TYPE;
                            MPIDI_PIP_fflush_task();
                            while (pip_global.local_compl_queue->head)
                                MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                        } else {
                            task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);

                            int socket_id;
                            if (in_cell) {
                                socket_id = cell->socket_id;
                                cell->pending = NULL;
                                task->cell = cell;
                                task->cell_queue = MPIDI_POSIX_mem_region.FreeQ[cell->my_rank];
                                task->asym_addr = (MPI_Aint) MPIDI_POSIX_asym_base_addr;
                                task->unexp_req = NULL;
                            } else {
                                socket_id = MPIDI_POSIX_REQUEST(sreq)->socket_id;
                                task->unexp_req = sreq;
                                task->cell = NULL;
                                MPIDI_POSIX_REQUEST_DEQUEUE(&sreq, prev_sreq,
                                                            MPIDI_POSIX_recvq_unexpected);
                            }

                            // task->rank = pip_global.local_rank;
                            task->send_flag = 0;
                            task->compl_flag = 0;
                            task->data_sz = data_sz;
                            // task->type = type;
                            task->next = NULL;
                            task->compl_next = NULL;

                            // task->task_id = pip_global.local_recv_counter[src_local]++;
                            // task->cur_task_id = pip_global.shm_recv_counter + src_local;
                            // task->completion_count = completion_count;

                            task->src = send_buffer;
                            task->dest = NULL;
                            task->segp =
                                (DLOOP_Segment *) MPIR_Handle_obj_alloc(&MPIDI_Segment_mem);
                            MPIR_Memcpy(task->segp, MPIDI_POSIX_REQUEST(req)->segment_ptr,
                                        sizeof(DLOOP_Segment));
                            task->segment_first = first;
                            MPIDI_PIP_Task_safe_enqueue(&pip_global.task_queue[socket_id], task);
                            MPIDI_PIP_Compl_task_enqueue(pip_global.local_compl_queue, task);
                            if (pip_global.local_compl_queue->task_num >= MPIDI_MAX_TASK_THREASHOLD) {
                                MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                            }
                        }

                        MPIDI_POSIX_REQUEST(req)->segment_first = last;
                    }
                } else {
                    char *recv_buffer =
                        (char *) MPIDI_POSIX_REQUEST(req)->user_buf +
                        (in_cell ? cell->addr_offset : MPIDI_POSIX_REQUEST(sreq)->addr_offset);
                    // void *recv_buffer = MPIDI_POSIX_REQUEST(req)->user_buf;
                    if (type == MPIDI_POSIX_TYPEEAGER) {
                        // MPIDI_PIP_fflush_compl_task(pip_global.local_recv_compl_queue);
                        // MPIDI_PIP_fflush_compl_task();
                        if (send_buffer) {
                            pip_global.copy_size += data_sz;
                            MPIR_Memcpy(recv_buffer, (void *) send_buffer, data_sz);
                        }
                        // eager_task_id = pip_global.local_recv_counter[src_local]++;
                    } else {
                        if (type == MPIDI_POSIX_TYPELMT_LAST) {
                            pip_global.copy_size += data_sz;
                            MPIR_Memcpy(recv_buffer, (void *) send_buffer, data_sz);
                            MPIDI_PIP_fflush_task();
                            while (pip_global.local_compl_queue->head)
                                MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                        } else {
                            // int src_local = MPIDI_POSIX_mem_region.local_ranks[grank];
                            int socket_id;
                            task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
                            if (in_cell) {
                                socket_id = cell->socket_id;
                                cell->pending = NULL;
                                task->cell = cell;
                                task->cell_queue = MPIDI_POSIX_mem_region.FreeQ[cell->my_rank];
                                task->asym_addr = (MPI_Aint) MPIDI_POSIX_asym_base_addr;
                                task->unexp_req = NULL;
                            } else {
                                socket_id = MPIDI_POSIX_REQUEST(sreq)->socket_id;
                                task->unexp_req = sreq;
                                task->cell = NULL;
                                MPIDI_POSIX_REQUEST_DEQUEUE(&sreq, prev_sreq,
                                                            MPIDI_POSIX_recvq_unexpected);
                            }

                            // task->rank = pip_global.local_rank;
                            task->send_flag = 0;
                            task->compl_flag = 0;
                            task->data_sz = data_sz;
                            // task->type = type;
                            task->next = NULL;
                            task->compl_next = NULL;
                            // task->completion_count = completion_count;
                            task->src = send_buffer;
                            task->dest = recv_buffer;
                            task->segp = NULL;
                            // task->task_id = pip_global.local_recv_counter[src_local]++;
                            // task->cur_task_id = pip_global.shm_recv_counter + src_local;
                            // task->compl_queue = pip_global.local_compl_queue;
                            MPIDI_PIP_Task_safe_enqueue(&pip_global.task_queue[socket_id], task);
                            MPIDI_PIP_Compl_task_enqueue(pip_global.local_compl_queue, task);
                            if (pip_global.local_compl_queue->task_num >= MPIDI_MAX_TASK_THREASHOLD) {
                                MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                            }
                        }
                    }
                }
                MPIDI_POSIX_REQUEST(req)->data_sz -= data_sz;
                // MPIDI_POSIX_REQUEST(req)->user_buf += data_sz;

                /* set status and dequeue receive request if done */
                count = MPIR_STATUS_GET_COUNT(req->status) + (MPI_Count) data_sz;
                MPIR_STATUS_SET_COUNT(req->status, count);
                if (type == MPIDI_POSIX_TYPEEAGER) {
                    if (in_cell) {
                        req->status.MPI_SOURCE = cell->rank;
                        req->status.MPI_TAG = cell->tag;
                    } else {
                        req->status.MPI_SOURCE = sreq->status.MPI_SOURCE;
                        req->status.MPI_TAG = sreq->status.MPI_TAG;
                    }

                    MPIDI_POSIX_REQUEST_DEQUEUE_AND_SET_ERROR(&req, prev_req,
                                                              MPIDI_POSIX_recvq_posted, mpi_errno);

                    (*completion_count)++;
                    goto release_cell_l;
                }

                if (type == MPIDI_POSIX_TYPELMT_LAST)
                    goto release_cell_l;
                else
                    goto fn_exit;
            }
            /* if matched  */
            prev_req = req;
            req = MPIDI_POSIX_REQUEST(req)->next;
        }

        /* unexpected message, no posted matching req */
        if (in_cell) {
            /* free the cell, move to unexpected queue */
            MPIR_Request *rreq;
            MPIDI_POSIX_REQUEST_CREATE_RREQ(rreq);
            MPIR_Object_set_ref(rreq, 1);
            /* set status */
            rreq->status.MPI_SOURCE = cell->rank;
            rreq->status.MPI_TAG = cell->tag;
            MPIR_STATUS_SET_COUNT(rreq->status, cell->pkt.mpich.datalen);
            MPIDI_POSIX_ENVELOPE_SET(MPIDI_POSIX_REQUEST(rreq), cell->rank, cell->tag,
                                     cell->context_id);
            data_sz = cell->pkt.mpich.datalen;
            MPIDI_POSIX_REQUEST(rreq)->data_sz = data_sz;
            MPIDI_POSIX_REQUEST(rreq)->type = cell->pkt.mpich.type;
            MPIDI_POSIX_REQUEST(rreq)->addr_offset = cell->addr_offset;
            MPIDI_POSIX_REQUEST(rreq)->socket_id = cell->socket_id;
            pip_global.copy_size += data_sz;
            if (data_sz > 0) {
                MPIDI_POSIX_REQUEST(rreq)->user_buf = (char *) MPL_malloc(data_sz, MPL_MEM_SHM);
                MPIR_Memcpy(MPIDI_POSIX_REQUEST(rreq)->user_buf, (void *) cell->pkt.mpich.p.payload,
                            data_sz);
            } else {
                MPIDI_POSIX_REQUEST(rreq)->user_buf = NULL;
            }

            MPIDI_POSIX_REQUEST(rreq)->datatype = MPI_BYTE;
            MPIDI_POSIX_REQUEST(rreq)->next = NULL;
            MPIDI_POSIX_REQUEST(rreq)->pending = cell->pending;
            /* enqueue rreq */
            MPIDI_POSIX_REQUEST_ENQUEUE(rreq, MPIDI_POSIX_recvq_unexpected);
            MPL_DBG_MSG_FMT(MPIR_DBG_HANDLE, TYPICAL,
                            (MPL_DBG_FDEST, "Unexpected from grank %d to %d in progress %d,%d,%d\n",
                             cell->my_rank, MPIDI_POSIX_mem_region.rank,
                             cell->rank, cell->tag, cell->context_id));
        } else {
            /* examine another message in unexpected queue */
            prev_sreq = sreq;
            sreq = MPIDI_POSIX_REQUEST(sreq)->next;
            goto unexpected_l;
        }
    }
  release_cell_l:

    if (in_cell) {
        /* release cell */
        MPL_DBG_MSG_FMT(MPIR_DBG_HANDLE, TYPICAL,
                        (MPL_DBG_FDEST, "Received from grank %d to %d in progress %d,%d,%d\n",
                         cell->my_rank, MPIDI_POSIX_mem_region.rank, cell->rank, cell->tag,
                         cell->context_id));
        cell->pending = NULL;
        {
            MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.FreeQ[cell->my_rank], cell);
        }

    } else {
        /* destroy unexpected req */
        MPIDI_POSIX_REQUEST(sreq)->pending = NULL;
        MPL_free(MPIDI_POSIX_REQUEST(sreq)->user_buf);
        MPIDI_POSIX_REQUEST_DEQUEUE_AND_SET_ERROR(&sreq, prev_sreq, MPIDI_POSIX_recvq_unexpected,
                                                  mpi_errno);
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* ----------------------------------------------------- */
/* MPIDI_POSIX_progress_send                     */
/* ----------------------------------------------------- */
#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_progress_send)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_send(int blocking, int *completion_count)
{
    int mpi_errno = MPI_SUCCESS;
    int dest, dest_local;
    MPIDI_POSIX_cell_ptr_t cell = NULL;
    MPIR_Request *sreq = MPIDI_POSIX_sendq.head;
    MPIR_Request *prev_sreq = NULL;
    MPIDI_PIP_task_t *task = NULL;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_SEND);

    if (sreq && !MPIDI_POSIX_queue_empty(MPIDI_POSIX_mem_region.my_freeQ)) {

        MPIDI_POSIX_queue_dequeue(MPIDI_POSIX_mem_region.my_freeQ, &cell);
        MPIDI_POSIX_ENVELOPE_GET(MPIDI_POSIX_REQUEST(sreq), cell->rank, cell->tag,
                                 cell->context_id);

        dest = MPIDI_POSIX_REQUEST(sreq)->dest;

        char *recv_buffer = (char *) cell->pkt.mpich.p.payload;
        size_t data_sz = MPIDI_POSIX_REQUEST(sreq)->data_sz;
        /*
         * TODO: make request field dest_lpid (or even recvQ[dest_lpid]) instead of dest - no need to do rank_to_lpid each time
         */
        int grank = MPIDI_CH4U_rank_to_lpid(dest, sreq->comm);
        dest_local = MPIDI_POSIX_mem_region.local_ranks[grank];
        cell->pending = NULL;

        if (MPIDI_POSIX_REQUEST(sreq)->type == MPIDI_POSIX_TYPESYNC) {
            /* increase req cc in order to release req only after ACK, do it once per SYNC request */
            /* non-NULL pending req signal receiver about sending ACK back */
            /* the pending req should be sent back for sender to decrease cc, for it is dequeued already */
            int c;
            cell->pending = sreq;
            MPIR_cc_incr(sreq->cc_ptr, &c);
            MPIDI_POSIX_REQUEST(sreq)->type = MPIDI_POSIX_TYPESTANDARD;
        }

        cell->addr_offset = MPIDI_POSIX_REQUEST(sreq)->addr_offset;
        if (data_sz <= MPIDI_POSIX_EAGER_THRESHOLD_32KB) {
            cell->pkt.mpich.datalen = data_sz;
            if (MPIDI_POSIX_REQUEST(sreq)->type == MPIDI_POSIX_TYPEACK) {
                cell->pkt.mpich.type = MPIDI_POSIX_TYPEACK;
                cell->pending = MPIDI_POSIX_REQUEST(sreq)->pending;
            } else {
                pip_global.copy_size += data_sz;
                if (MPIDI_POSIX_REQUEST(sreq)->segment_ptr) {
                    /* non-contig */
                    MPIR_Segment_pack(MPIDI_POSIX_REQUEST(sreq)->segment_ptr,
                                      MPIDI_POSIX_REQUEST(sreq)->segment_first,
                                      (MPI_Aint *) & MPIDI_POSIX_REQUEST(sreq)->segment_size,
                                      recv_buffer);
                    MPIR_Segment_free(MPIDI_POSIX_REQUEST(sreq)->segment_ptr);
                } else {
                    /* contig */
                    MPIR_Memcpy((void *) recv_buffer, MPIDI_POSIX_REQUEST(sreq)->user_buf, data_sz);
                }

                /* Enqueue task without performing actual copy */
                cell->pkt.mpich.type = MPIDI_POSIX_TYPEEAGER;
                /* set status */
                /*
                 * TODO: incorrect count for LMT - set to a last chunk of data
                 * is send status required?
                 */
                sreq->status.MPI_SOURCE = cell->rank;
                sreq->status.MPI_TAG = cell->tag;
                MPIR_STATUS_SET_COUNT(sreq->status, data_sz);
            }

            MPIDI_POSIX_REQUEST_DEQUEUE_AND_SET_ERROR(&sreq, prev_sreq, MPIDI_POSIX_sendq,
                                                      mpi_errno);
            (*completion_count)++;

            MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.RecvQ[grank], cell);
        } else {
            size_t sz_thsd;
            if (MPIDI_POSIX_REQUEST(sreq)->data_sz < MPIDI_POSIX_CELL_SWITCH_THRESHOLD) {
                /* 32KB cell size */
                sz_thsd = MPIDI_POSIX_EAGER_THRESHOLD_32KB;
            } else {
                /* 64KB cell size */
                sz_thsd = MPIDI_POSIX_EAGER_THRESHOLD;
            }
            cell->pkt.mpich.datalen = sz_thsd;
            MPIDI_POSIX_REQUEST(sreq)->data_sz -= sz_thsd;
            MPIDI_POSIX_REQUEST(sreq)->addr_offset += sz_thsd;
            if (MPIDI_POSIX_REQUEST(sreq)->data_sz > sz_thsd) {
                /* need more LMT send calls */
                cell->pkt.mpich.type = MPIDI_POSIX_TYPELMT;
                task = (MPIDI_PIP_task_t *) MPIR_Handle_obj_alloc(&MPIDI_Task_mem);
                task->cell = cell;

                task->compl_flag = 0;
                task->asym_addr = (MPI_Aint) MPIDI_POSIX_asym_base_addr;

                task->send_flag = 1;
                task->next = NULL;
                task->compl_next = NULL;
                task->unexp_req = NULL;
                // task->rank = pip_global.local_rank;
                task->data_sz = sz_thsd;

                // task->cur_task_id = pip_global.shm_send_counter + dest_local;
                // task->task_id = pip_global.local_send_counter[dest_local]++;
                task->cell_queue = MPIDI_POSIX_mem_region.RecvQ[grank];
                if (MPIDI_POSIX_REQUEST(sreq)->segment_ptr) {
                    /* non-contig */
                    // size_t last =
                    //     MPIDI_POSIX_REQUEST(sreq)->segment_first + MPIDI_POSIX_EAGER_THRESHOLD;
                    // MPIR_Segment_pack(MPIDI_POSIX_REQUEST(sreq)->segment_ptr,
                    //                   MPIDI_POSIX_REQUEST(sreq)->segment_first, (MPI_Aint *) & last,
                    //                   recv_buffer);
                    // MPIDI_POSIX_REQUEST(sreq)->segment_first = last;
                    // MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.RecvQ[grank], cell);
                    // MPIR_Handle_obj_free(&MPIDI_Task_mem, task);
                    // goto fn_exit;
                    // printf("rank %d - Sender sends non-contiguous data\n", pip_global.local_rank);
                    // fflush(stdout);
                    size_t last = MPIDI_POSIX_REQUEST(sreq)->segment_first + sz_thsd;
                    task->segp = (DLOOP_Segment *) MPIR_Handle_obj_alloc(&MPIDI_Segment_mem);
                    task->segment_first = MPIDI_POSIX_REQUEST(sreq)->segment_first;
                    MPIR_Memcpy(task->segp, MPIDI_POSIX_REQUEST(sreq)->segment_ptr,
                                sizeof(DLOOP_Segment));
                    MPIR_Segment_manipulate(MPIDI_POSIX_REQUEST(sreq)->segment_ptr, MPIDI_POSIX_REQUEST(sreq)->segment_first, &last, NULL,      /* contig fn */
                                            NULL,       /* vector fn */
                                            NULL,       /* blkidx fn */
                                            NULL,       /* index fn */
                                            NULL, NULL);
                    MPIDI_POSIX_REQUEST(sreq)->segment_first = last;

                    task->src = NULL;
                    task->dest = recv_buffer;
                } else {
                    task->segp = NULL;
                    task->src = MPIDI_POSIX_REQUEST(sreq)->user_buf;
                    task->dest = recv_buffer;
                    MPIDI_POSIX_REQUEST(sreq)->user_buf += sz_thsd;
                }

                MPIDI_PIP_Task_safe_enqueue(&pip_global.task_queue[cell->socket_id], task);
                MPIDI_PIP_Compl_task_enqueue(pip_global.local_compl_queue, task);

                if (pip_global.local_compl_queue->task_num >= MPIDI_MAX_TASK_THREASHOLD) {
                    MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                }
            } else {
                /* only one LMT send left */
                /* long message */

                pip_global.copy_size += sz_thsd;
                cell->pkt.mpich.type = MPIDI_POSIX_TYPELMT_LAST;
                if (MPIDI_POSIX_REQUEST(sreq)->segment_ptr) {
                    /* non-contig */
                    size_t last = MPIDI_POSIX_REQUEST(sreq)->segment_first + sz_thsd;
                    MPIR_Segment_pack(MPIDI_POSIX_REQUEST(sreq)->segment_ptr,
                                      MPIDI_POSIX_REQUEST(sreq)->segment_first, (MPI_Aint *) & last,
                                      recv_buffer);
                    MPIDI_POSIX_REQUEST(sreq)->segment_first = last;
                    // MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.RecvQ[grank], cell);
                    // goto fn_exit;
                } else {
                    /* contig */
                    MPIR_Memcpy((void *) recv_buffer, MPIDI_POSIX_REQUEST(sreq)->user_buf, sz_thsd);
                    MPIDI_POSIX_REQUEST(sreq)->user_buf += sz_thsd;
                }

                MPIDI_PIP_fflush_task();
                while (pip_global.local_compl_queue->head)
                    MPIDI_PIP_fflush_compl_task(pip_global.local_compl_queue);
                MPIDI_POSIX_queue_enqueue(MPIDI_POSIX_mem_region.RecvQ[grank], cell);
            }

        }

        // (*completion_count)++;
    } else if (pip_global.recv_empty) {

        if (pip_global.task_queue[pip_global.local_numa_id].head) {
            MPIDI_PIP_exec_task(&pip_global.task_queue[pip_global.local_numa_id]);
        } else {
            int i;
            for (i = 0; i < pip_global.numa_max_node; ++i) {
                if (pip_global.task_queue[i].head && i != pip_global.local_numa_id) {
                    MPIDI_PIP_exec_task(&pip_global.task_queue[i]);
                    goto fn_exit;
                }
            }
            /* Steal from others */

            MPIDI_PIP_steal_task();
        }
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_DO_PROGRESS_SEND);
    return mpi_errno;
}

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_POSIX_progress)
MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress(int blocking)
{
    int complete = 0;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_POSIX_PROGRESS);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_POSIX_PROGRESS);

    do {
        /* Receieve progress */
        MPID_THREAD_CS_ENTER(POBJ, MPIDI_POSIX_SHM_MUTEX);
        MPIDI_POSIX_progress_recv(blocking, &complete);
        MPID_THREAD_CS_EXIT(POBJ, MPIDI_POSIX_SHM_MUTEX);
        /* Send progress */
        MPID_THREAD_CS_ENTER(POBJ, MPIDI_POSIX_SHM_MUTEX);
        MPIDI_POSIX_progress_send(blocking, &complete);
        MPID_THREAD_CS_EXIT(POBJ, MPIDI_POSIX_SHM_MUTEX);

        if (complete > 0)
            break;
    } while (blocking);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_POSIX_PROGRESS);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_test(void)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_poke(void)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_POSIX_progress_start(MPID_Progress_state * state)
{
    MPIR_Assert(0);
    return;
}

MPL_STATIC_INLINE_PREFIX void MPIDI_POSIX_progress_end(MPID_Progress_state * state)
{
    MPIR_Assert(0);
    return;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_wait(MPID_Progress_state * state)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_register(int (*progress_fn) (int *))
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_deregister(int id)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_activate(int id)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

MPL_STATIC_INLINE_PREFIX int MPIDI_POSIX_progress_deactivate(int id)
{
    MPIR_Assert(0);
    return MPI_SUCCESS;
}

#endif /* POSIX_PROGRESS_H_INCLUDED */
