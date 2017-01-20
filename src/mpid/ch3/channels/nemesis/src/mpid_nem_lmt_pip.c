/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * (C) 2016 by Argonne National Laboratory.
 *     See COPYRIGHT in top-level directory.
 */

#include "mpid_nem_impl.h"
#include "mpid_nem_datatypes.h"

#ifdef HAVE_PIP
/* #define PIP_DBG_PRINT(str,...) do {fprintf(stdout, str, ## __VA_ARGS__);fflush(stdout);} while (0)*/
#define PIP_DBG_PRINT(str,...) do {} while (0)

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

    MPID_nem_lmt_send_RTS(vc, rts_pkt, NULL, 0);

    {
        PIP_DBG_PRINT("[%d] %s: issued RTS: extpkt %p, sbuf[%p,%ld,0x%lx, sz %ld], sreq 0x%lx\n",
                      vc->pg_rank, __FUNCTION__, rts_pkt->extpkt, rts_pkt->extpkt->sender_buf,
                      rts_pkt->extpkt->sender_count, rts_pkt->extpkt->sender_dt, rts_pkt->data_sz,
                      rts_pkt->sender_req_id);
    }

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_INITIATE_LMT);
    return mpi_errno;
  fn_fail:
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

/* called in CTS handler (unused in PIP) */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_start_send
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_start_send(MPIDI_VC_t * vc, MPIR_Request * req, MPL_IOV r_cookie)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);

    /* Do nothing, always sender issues only RTS. */

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
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);

    /* Copy data to rbuf. */
    mpi_errno = MPIR_Localcopy(rreq->ch.lmt_buf_addr, rreq->ch.lmt_count,
                               rreq->ch.lmt_datatype, rreq->dev.user_buf,
                               rreq->dev.user_count, rreq->dev.datatype);
    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    PIP_DBG_PRINT("[%d] %s: copied: sbuf[%p,%ld,0x%lx] rbuf[%p,%ld,0x%lx], sreq 0x%lx\n",
                  vc->pg_rank, __FUNCTION__, rreq->ch.lmt_buf_addr, rreq->ch.lmt_count,
                  rreq->ch.lmt_datatype, rreq->dev.user_buf, rreq->dev.user_count,
                  rreq->dev.datatype, rreq->ch.lmt_req_id);

    /* Issue DONE */
    MPID_nem_lmt_send_DONE(vc, rreq);
    PIP_DBG_PRINT("[%d] issue DONE\n", vc->pg_rank);

    mpi_errno = MPID_Request_complete(rreq);
    if (mpi_errno != MPI_SUCCESS)
        MPIR_ERR_POP(mpi_errno);
    PIP_DBG_PRINT("[%d] complete rreq %p/0x%lx\n", vc->pg_rank, rreq, rreq->handle);

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
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_HANDLE_COOKIE);
    return MPI_SUCCESS;
}

/* called in DONE hanler on receiver (unused in PIP) */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_done_recv
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_done_recv(MPIDI_VC_t * vc, MPIR_Request * rreq)
{
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_DONE_RECV);
    return MPI_SUCCESS;
}

/* called in DONE handler on sender) */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_done_send
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_done_send(MPIDI_VC_t * vc, MPIR_Request * sreq)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);

    PIP_DBG_PRINT("[%d] %s: complete sreq %p/0x%lx, free extpkt=%p\n",
                  vc->pg_rank, __FUNCTION__, sreq, sreq->handle,
                  sreq->ch.lmt_extpkt);

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
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);

    /* Do nothing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_PROGRESS);
    return MPI_SUCCESS;
}
#endif /* end of HAVE_PIP */
