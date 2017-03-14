/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * (C) 2016 by Argonne National Laboratory.
 *     See COPYRIGHT in top-level directory.
 */

#include "mpid_nem_impl.h"
#include "mpid_nem_inline.h"
#include "mpid_nem_datatypes.h"

/*
=== BEGIN_MPI_T_CVAR_INFO_BLOCK ===

cvars:
    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD
      category    : NEMESIS
      type        : int
      default     : 131072
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        Messages larger than this size will use the parallel-copy method
        for intranode PIP LMT implementation, set to 0 to disable it.

=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

#ifdef HAVE_PIP

#ifdef LMT_PIP_DBG
static int myrank = -1;         /* debug purpose */
#define PIP_DBG_PRINT(str,...) do {fprintf(stdout, str, ## __VA_ARGS__);fflush(stdout);} while (0)
#else
#define PIP_DBG_PRINT(str,...) do {} while (0)
#endif

/* called in MPID_nem_lmt_RndvSend */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_initiate_lmt
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_initiate_lmt(MPIDI_VC_t * vc, MPIDI_CH3_Pkt_t * pkt, MPIR_Request * req)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_pkt_lmt_rts_t *const rts_pkt = (MPID_nem_pkt_lmt_rts_t *) pkt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_INITIATE_LMT);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_INITIATE_LMT);

    MPIR_CHKPMEM_DECL(1);

#ifdef LMT_PIP_DBG
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
#endif

    /* use extent packet to transfer PIP LMT metadata, because increased packet
     * size can also effect all other messages. The peer PIP can directly access
     * the extent packet.*/
    MPIR_CHKPMEM_MALLOC(rts_pkt->extpkt, MPID_nem_pkt_lmt_rts_pipext_t *,
                        sizeof(MPID_nem_pkt_lmt_rts_pipext_t), mpi_errno, "lmt RTS extent packet");

    rts_pkt->extpkt->sender_buf = (uintptr_t) req->dev.user_buf;
    rts_pkt->extpkt->sender_dt = req->dev.datatype;
    rts_pkt->extpkt->sender_count = req->dev.user_count;

    OPA_store_int(&rts_pkt->extpkt->pcp.stat, MPIDI_NEM_LMT_PIP_PCP_INIT);
    req->ch.lmt_extpkt = rts_pkt->extpkt;       /* store in request, thus can free it
                                                 * when LMT done.*/
    OPA_write_barrier();

    MPID_nem_lmt_send_RTS(vc, rts_pkt, NULL, 0);

    PIP_DBG_PRINT("[%d] %s: issued RTS: extpkt %p, sbuf[0x%lx,%ld,0x%lx, sz %ld], sreq 0x%lx\n",
                  myrank, __FUNCTION__, rts_pkt->extpkt, rts_pkt->extpkt->sender_buf,
                  rts_pkt->extpkt->sender_count, (unsigned long) rts_pkt->extpkt->sender_dt,
                  rts_pkt->data_sz, (unsigned long) rts_pkt->sender_req_id);

    MPIR_CHKPMEM_COMMIT();

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_INITIATE_LMT);
    return mpi_errno;
  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_vc_terminated
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_vc_terminated(MPIDI_VC_t * vc)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_VC_TERMINATED);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_VC_TERMINATED);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_VC_TERMINATED);
    return mpi_errno;
}

/* called in CTS handler */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_start_send
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_start_send(MPIDI_VC_t * vc, MPIR_Request * req, MPL_IOV r_cookie)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint send_true_lb = 0, send_true_extent ATTRIBUTE((unused));
    MPI_Aint recv_true_lb = 0, recv_true_extent ATTRIBUTE((unused));
    int send_iscontig ATTRIBUTE((unused)), recv_iscontig ATTRIBUTE((unused));
    char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
    int old_pcp_stat = MPID_NEM_LMT_PIP_PCP_P1_COPY;
    MPI_Aint copy_size = 0, data_size = 0, recv_size = 0;
    MPIDU_Datatype *send_dtptr, *recv_dtptr;

    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) req->ch.lmt_extpkt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);

    /* Received CTS only when parallel copy is initialized. */

    /* Check whether sender is already copying part-2. */
    old_pcp_stat = OPA_cas_int(&lmt_extpkt->pcp.stat, MPID_NEM_LMT_PIP_PCP_P1_COPY,
                               MPID_NEM_LMT_PIP_PCP_P2_COPY);

    /* Copy part-2 if receiver does not start yet.
     * Current stat can be PI_COPY | P2_COPY | DONE.*/
    if (old_pcp_stat == MPID_NEM_LMT_PIP_PCP_P1_COPY) {

        PIP_DBG_PRINT("[%d] %s: extpkt %p, recv_buf=0x%lx, count=%ld, datatype=%lx\n",
                      myrank, __FUNCTION__, lmt_extpkt, lmt_extpkt->pcp.receiver_buf,
                      lmt_extpkt->pcp.receiver_count, (unsigned long) lmt_extpkt->pcp.receiver_dt);

        MPIDI_Datatype_get_info(req->dev.user_count, req->dev.datatype,
                                send_iscontig, data_size, send_dtptr, send_true_lb);
        MPIDI_Datatype_get_info(lmt_extpkt->pcp.receiver_count, lmt_extpkt->pcp.receiver_dt,
                                recv_iscontig, recv_size, recv_dtptr, recv_true_lb);

        copy_size = data_size / 2;
        sbuf_ptr = (char *) lmt_extpkt->sender_buf + copy_size;
        rbuf_ptr = (char *) lmt_extpkt->pcp.receiver_buf + copy_size;
        copy_size = data_size - copy_size;      /* remaining half */

        PIP_DBG_PRINT("[%d] parallel-copy(s): copying part-2, data_size=%ld/%ld, "
                      "sbuf_ptr=%p(%ld), rbuf_ptr=%p(%ld)\n", myrank, copy_size,
                      data_size, sbuf_ptr, send_true_lb, rbuf_ptr, recv_true_lb);

        MPIR_Memcpy(((char *) rbuf_ptr + recv_true_lb), ((char *) sbuf_ptr + send_true_lb),
                    copy_size);

        /* Sender side copy DONE. */
        OPA_store_int(&lmt_extpkt->pcp.stat, MPID_NEM_LMT_PIP_PCP_SDONE);
        PIP_DBG_PRINT("[%d] parallel-copy(s): part-2 DONE\n", myrank);
    }

    /* Wait till sender set DONE. */
    while (OPA_load_int(&lmt_extpkt->pcp.stat) != MPID_NEM_LMT_PIP_PCP_DONE);
    PIP_DBG_PRINT("[%d] parallel-copy(s) DONE\n", myrank);

    /* Complete send request. */
    MPID_nem_lmt_pip_done_send(vc, req);

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    return mpi_errno;
}

/* called in RTS handler or RndvRecv */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_start_recv
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_start_recv(MPIDI_VC_t * vc, MPIR_Request * rreq, MPL_IOV s_cookie)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint recv_size = 0, data_size = 0;
    int send_iscontig = 0, recv_iscontig = 0;
    MPI_Aint send_true_lb = 0, send_true_extent ATTRIBUTE((unused));
    MPI_Aint recv_true_lb = 0, recv_true_extent ATTRIBUTE((unused));
    MPIDU_Datatype *send_dtptr, *recv_dtptr;

    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) rreq->ch.lmt_extpkt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);

#ifdef LMT_PIP_DBG
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
#endif

    MPIDI_Datatype_get_info(lmt_extpkt->sender_count, lmt_extpkt->sender_dt,
                            send_iscontig, data_size, send_dtptr, send_true_lb);
    MPIDI_Datatype_get_info(rreq->dev.user_count, rreq->dev.datatype,
                            recv_iscontig, recv_size, recv_dtptr, recv_true_lb);

    /* Single copy for medium-size message. */
    if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD == 0 ||
        data_size < MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD ||
        /* TODO: Implement noncontig parallel copy */
        !send_iscontig || !recv_iscontig) {

        PIP_DBG_PRINT("[%d] start single-copy, extpkt %p, data_size=%ld, "
                      "sender_count=%ld, send_iscontig=%d, receiver_count=%ld, recv_iscontig=%d\n",
                      myrank, lmt_extpkt, data_size, lmt_extpkt->sender_count,
                      send_iscontig, rreq->dev.user_count, recv_iscontig);

        mpi_errno = MPIR_Localcopy((const void *) lmt_extpkt->sender_buf, lmt_extpkt->sender_count,
                                   lmt_extpkt->sender_dt, rreq->dev.user_buf,
                                   rreq->dev.user_count, rreq->dev.datatype);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);

        /* DONE, notify sender. */
        MPID_nem_lmt_send_DONE(vc, rreq);
        PIP_DBG_PRINT("[%d] issue single-copy DONE, data_size=%ld\n", myrank, data_size);
    }
    /* Parallel copy for large message. */
    else {
        char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
        int old_pcp_stat = MPID_NEM_LMT_PIP_PCP_P1_COPY;
        MPI_Aint copy_size = 0;

        OPA_store_int(&lmt_extpkt->pcp.stat, MPID_NEM_LMT_PIP_PCP_P1_COPY);

        /* Sync with sender to initial parallel copy. */
        lmt_extpkt->pcp.receiver_buf = (uintptr_t) rreq->dev.user_buf;
        lmt_extpkt->pcp.receiver_count = rreq->dev.user_count;
        lmt_extpkt->pcp.receiver_dt = rreq->dev.datatype;
        OPA_write_barrier();

        MPID_nem_lmt_send_CTS(vc, rreq, NULL, 0);

        copy_size = data_size / 2;
        sbuf_ptr = (char *) lmt_extpkt->sender_buf;
        rbuf_ptr = (char *) rreq->dev.user_buf;

        PIP_DBG_PRINT("[%d] parallel-copy(r): copying part-1, data_size=%ld/%ld, "
                      "sbuf_ptr=%p(%ld), rbuf_ptr=%p(%ld)\n", myrank, copy_size,
                      data_size, sbuf_ptr, send_true_lb, rbuf_ptr, recv_true_lb);

        MPIR_Memcpy(((char *) rbuf_ptr + recv_true_lb), ((char *) sbuf_ptr + send_true_lb),
                    copy_size);

        /* Check whether sender is already copying part-2.
         * Current stat can be PI_COPY | P2_COPY | SDONE.*/
        old_pcp_stat = OPA_cas_int(&lmt_extpkt->pcp.stat, MPID_NEM_LMT_PIP_PCP_P1_COPY,
                                   MPID_NEM_LMT_PIP_PCP_P2_COPY);

        /* Copy part-2 if sender does not start yet. */
        if (old_pcp_stat == MPID_NEM_LMT_PIP_PCP_P1_COPY) {
            sbuf_ptr += copy_size;
            rbuf_ptr += copy_size;
            copy_size = data_size - copy_size;

            PIP_DBG_PRINT("[%d] parallel-copy(r): copying part-2, data_size=%ld/%ld, "
                          "sbuf_ptr=%p(%ld), rbuf_ptr=%p(%ld)\n", myrank, copy_size,
                          data_size, sbuf_ptr, send_true_lb, rbuf_ptr, recv_true_lb);

            MPIR_Memcpy(((char *) rbuf_ptr + recv_true_lb), ((char *) sbuf_ptr + send_true_lb),
                        copy_size);
        }
        /* Otherwise wait sender finish. */
        else {
            while (OPA_load_int(&lmt_extpkt->pcp.stat) != MPID_NEM_LMT_PIP_PCP_SDONE);
        }

        /* entire copy DONE. */
        OPA_store_int(&lmt_extpkt->pcp.stat, MPID_NEM_LMT_PIP_PCP_DONE);
        PIP_DBG_PRINT("[%d] parallel-copy(r) DONE\n", myrank);
    }

    /* Complete receive request. */
    MPID_nem_lmt_pip_done_recv(vc, rreq);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_handle_cookie
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_handle_cookie(MPIDI_VC_t * vc, MPIR_Request * req, MPL_IOV cookie)
{
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);
    return MPI_SUCCESS;
}

/* Called in start_recv on receiver. */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_done_recv
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_done_recv(MPIDI_VC_t * vc, MPIR_Request * rreq)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);

    mpi_errno = MPID_Request_complete(rreq);
    if (mpi_errno != MPI_SUCCESS)
        MPIR_ERR_POP(mpi_errno);
    PIP_DBG_PRINT("[%d] %s: complete rreq %p/0x%x\n", myrank, __FUNCTION__, rreq, rreq->handle);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

/* Called in sender DONE handler for single-copy LMT,
 * and in start_send for parallel-copy LMT.  */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_done_send
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_done_send(MPIDI_VC_t * vc, MPIR_Request * sreq)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);

    PIP_DBG_PRINT("[%d] %s: complete sreq %p/0x%x, free extpkt=%p\n",
                  myrank, __FUNCTION__, sreq, sreq->handle, sreq->ch.lmt_extpkt);

    MPIR_Assert(sreq->ch.lmt_extpkt);
    MPL_free(sreq->ch.lmt_extpkt);

    mpi_errno = MPID_Request_complete(sreq);
    if (mpi_errno != MPI_SUCCESS)
        MPIR_ERR_POP(mpi_errno);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}


#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_progress
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_progress(void)
{
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);
    return MPI_SUCCESS;
}
#endif /* end of HAVE_PIP */
