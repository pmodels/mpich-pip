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

    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_PCP_CHUNKSIZE
      category    : NEMESIS
      type        : int
      default     : 131072
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        Divide message into multiple chunks each with this size in parallel-copy
        method for intranode PIP LMT implementation, set to 0 to always divide
        into two chunks. If a message is larger than pcp threshold but smaller
        than chunk size, then divide it into two chunks.

    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_THRESHOLD
      category    : NEMESIS
      type        : int
      default     : 131072
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        Noncontiguous messages larger than this size will use the parallel-copy
        method for intranode PIP LMT implementation, set to 0 to disable it.

    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE
      category    : NEMESIS
      type        : int
      default     : 131072
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        Divide noncontiguous message into multiple chunks each with this size in
        parallel-copy method for intranode PIP LMT implementation, set to 0 to
        always divide into two chunks. If a message is larger than pcp threshold
        but smaller than chunk size, then divide it into two chunks.

    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKEXTENT
      category    : NEMESIS
      type        : int
      default     : 0
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        Divide noncontiguous message into multiple chunks each with this extent
        in parallel-copy method for intranode PIP LMT implementation, set to 0 to
        ignore this option.
=== END_MPI_T_CVAR_INFO_BLOCK ===
*/

#ifdef HAVE_PIP
//#define LMT_PIP_DBG
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

    rts_pkt->extpkt->pcp.sender_buf = (uintptr_t) req->dev.user_buf;
    rts_pkt->extpkt->pcp.sender_dt = req->dev.datatype;
    rts_pkt->extpkt->pcp.sender_count = req->dev.user_count;

    OPA_store_int(&rts_pkt->extpkt->pcp.offset, 0);
    OPA_store_int(&rts_pkt->extpkt->pcp.complete_cnt, 0);

    req->ch.lmt_extpkt = rts_pkt->extpkt;       /* store in request, thus can free it
                                                 * when LMT done.*/
    OPA_write_barrier();

    MPID_nem_lmt_send_RTS(vc, rts_pkt, NULL, 0);

    PIP_DBG_PRINT("[%d] %s: issued RTS: extpkt %p, sbuf[0x%lx,%ld,0x%lx, sz %ld], sreq 0x%lx\n",
                  myrank, __FUNCTION__, rts_pkt->extpkt, rts_pkt->extpkt->pcp.sender_buf,
                  rts_pkt->extpkt->pcp.sender_count, (unsigned long) rts_pkt->extpkt->pcp.sender_dt,
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

#undef FUNCNAME
#define FUNCNAME lmt_pip_copy_nchunked_contig
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline void lmt_pip_copy_nchunked_contig(MPID_nem_pkt_lmt_rts_pipext_t * lmt_extpkt,
                                                MPI_Aint data_size, char *sbuf, char *rbuf,
                                                const char *dbg_nm)
{
    char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
    int offset = 0;
    MPI_Aint copy_size = 0;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_COPY_NCHUNKED_CONTIG);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_COPY_NCHUNKED_CONTIG);

    offset = OPA_fetch_and_incr_int(&lmt_extpkt->pcp.offset);
    while (offset < lmt_extpkt->pcp.nchunks) {
        copy_size = lmt_extpkt->pcp.chunk_size;
        if (offset == lmt_extpkt->pcp.nchunks - 1 && data_size % lmt_extpkt->pcp.chunk_size) {
            copy_size = data_size % lmt_extpkt->pcp.chunk_size;
        }
        sbuf_ptr = sbuf + lmt_extpkt->pcp.chunk_size * offset;
        rbuf_ptr = rbuf + lmt_extpkt->pcp.chunk_size * offset;

        PIP_DBG_PRINT("[%d] parallel-copy(%s): copying part-%d/%d, data_size=%ld/%ld, "
                      "sbuf_ptr=%p, rbuf_ptr=%p\n", myrank, dbg_nm, offset,
                      lmt_extpkt->pcp.nchunks, copy_size, data_size, sbuf_ptr, rbuf_ptr);

        MPIR_Memcpy(rbuf_ptr, sbuf_ptr, copy_size);

        /* Finished a chunk. */
        OPA_decr_int(&lmt_extpkt->pcp.complete_cnt);

        /* Get next chunk. */
        offset = OPA_fetch_and_incr_int(&lmt_extpkt->pcp.offset);
    }

    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_COPY_NCHUNKED_CONTIG);
}

#undef FUNCNAME
#define FUNCNAME lmt_pip_copy_nchunked_noncontig
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline void lmt_pip_copy_nchunked_noncontig(MPID_nem_pkt_lmt_rts_pipext_t * lmt_extpkt,
                                                   MPI_Aint data_size ATTRIBUTE((unused)),
                                                   char *sbuf, char *rbuf, const char *dbg_nm)
{
    char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
    MPI_Aint copy_size = 0, contig_offset = 0;
    int cur_chunk = 0, copied = 0;
    int cur_blk = 0, blk_sta = 0, blk_end = 0;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_COPY_NCHUNKED_NONCONTIG);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_COPY_NCHUNKED_NONCONTIG);

    cur_chunk = OPA_fetch_and_incr_int(&lmt_extpkt->pcp.offset);
    while (cur_chunk < lmt_extpkt->pcp.nchunks) {

        /* Calculate block range */
        blk_sta = lmt_extpkt->pcp.block_chunks[cur_chunk];
        if (cur_chunk == lmt_extpkt->pcp.nchunks - 1) {
            blk_end = lmt_extpkt->pcp.nblocks - 1;
        }
        else {
            blk_end = lmt_extpkt->pcp.block_chunks[cur_chunk + 1] - 1;
        }

        switch (lmt_extpkt->pcp.type) {
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED:
            {
                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    copy_size = lmt_extpkt->pcp.noncontig_blocks[cur_blk].size;
                    contig_offset = copy_size * cur_blk;
                    sbuf_ptr = sbuf + lmt_extpkt->pcp.noncontig_blocks[cur_blk].offset;
                    rbuf_ptr = rbuf + contig_offset;

                    MPIR_Memcpy(rbuf_ptr, sbuf_ptr, copy_size);
                }
            }
            break;
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED:
            {
                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    copy_size = lmt_extpkt->pcp.noncontig_blocks[cur_blk].size;
                    contig_offset = copy_size * cur_blk;
                    sbuf_ptr = sbuf + contig_offset;
                    rbuf_ptr = rbuf + lmt_extpkt->pcp.noncontig_blocks[cur_blk].offset;

                    MPIR_Memcpy(rbuf_ptr, sbuf_ptr, copy_size);
                }
            }
            break;
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED:
            {
                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    copy_size = lmt_extpkt->pcp.noncontig_blocks[cur_blk].size;
                    sbuf_ptr = sbuf + lmt_extpkt->pcp.noncontig_blocks[cur_blk].offset;
                    rbuf_ptr = rbuf + lmt_extpkt->pcp.noncontig_blocks[cur_blk].offset;

                    MPIR_Memcpy(rbuf_ptr, sbuf_ptr, copy_size);
                }

                PIP_DBG_PRINT
                    ("[%d] parallel-copy(%s): copying noncontig, nchunks=%d/%d, nblocks=(%d - %d)/%d, type=%d,"
                     "offset=%ld, copy_size=%ld(%ld)\n", myrank, dbg_nm, cur_chunk,
                     lmt_extpkt->pcp.nchunks, blk_sta, blk_end, lmt_extpkt->pcp.nblocks,
                     lmt_extpkt->pcp.type, lmt_extpkt->pcp.noncontig_blocks[cur_chunk].offset,
                     copy_size * (blk_end - blk_sta + 1), copy_size);
            }
            break;
        default:
            MPIR_Assert(lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED
                        || lmt_extpkt->pcp.type ==
                        MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED ||
                        lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED);
            break;
        }

        /* Finished a chunk. */
        OPA_decr_int(&lmt_extpkt->pcp.complete_cnt);

        /* Get next chunk. */
        cur_chunk = OPA_fetch_and_incr_int(&lmt_extpkt->pcp.offset);
        copied++;
    }

    PIP_DBG_PRINT
        ("[%d] parallel-copy(%s): noncontig copy DONE. copied_nchunks=%d/%d, data_size=%ld, "
         "sbuf_ptr=%p, rbuf_ptr=%p\n", myrank, dbg_nm, copied, lmt_extpkt->pcp.nchunks, data_size,
         sbuf_ptr, rbuf_ptr);

    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_COPY_NCHUNKED_NONCONTIG);
}

#undef FUNCNAME
#define FUNCNAME lmt_pip_gen_datatype_blocks
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline int lmt_pip_gen_datatype_blocks(void *buf, MPI_Aint count, MPI_Datatype datatype,
                                              int *nblks_ptr,
                                              MPID_nem_lmt_pip_pcp_noncontig_block_t ** blks_ptr,
                                              int *nchunks_ptr, int **blk_chunks_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    int depth = 0, max_depth = 0;
    MPID_nem_lmt_pip_pcp_noncontig_block_t *blks = NULL;
    int nblks = 1, cur_blk = 0;
    MPI_Aint cur_offset = 0, cur_chunk_size = 0;
    struct DLOOP_Dataloop *last_loop_p = NULL;
    int last_kind = 0;
    struct MPIDU_Segment *segment_ptr = NULL;
    int nchunks = 0, *blk_chunks = NULL;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_GEN_DATATYPE_BLOCKS);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_GEN_DATATYPE_BLOCKS);
    MPIR_CHKPMEM_DECL(2);

    segment_ptr = MPIDU_Segment_alloc();
    MPIR_ERR_CHKANDJUMP1((segment_ptr == NULL), mpi_errno, MPI_ERR_OTHER,
                         "**nomem", "**nomem %s", "MPIDU_Segment_alloc");
    MPIDU_Segment_init(buf, count, datatype, segment_ptr, 0);

    for (depth = 0; depth < DLOOP_MAX_DATATYPE_DEPTH; depth++) {
        int kind = segment_ptr->stackelm[depth].loop_p->kind;

        /* FIXME: The following routine does not support irregular structure. */
        if (!(kind & DLOOP_KIND_CONTIG) && !(kind & DLOOP_KIND_VECTOR)) {
            (*nblks_ptr) = 0;
            (*blks_ptr) = NULL;
            goto fn_exit;
        }

        nblks *= segment_ptr->stackelm[depth].loop_p->loop_params.count;
        if (kind & DLOOP_FINAL_MASK) {
            last_kind = kind;
            break;
        }
    }
    max_depth = depth;

    MPIR_CHKPMEM_MALLOC(blks, MPID_nem_lmt_pip_pcp_noncontig_block_t *,
                        sizeof(MPID_nem_lmt_pip_pcp_noncontig_block_t) * nblks,
                        mpi_errno, "lmt PIP blks");
    MPIR_CHKPMEM_MALLOC(blk_chunks, int *, sizeof(int) * nblks, mpi_errno, "lmt PIP blk_chunks");

    cur_blk = 0;
    cur_offset = 0;
    last_loop_p = segment_ptr->stackelm[max_depth].loop_p;

    nchunks = 1;        /* at least one chunk */
    cur_chunk_size = 0;
    blk_chunks[0] = 0;

    do {
        DLOOP_Offset stride = 0;
        blks[cur_blk].offset = cur_offset;

        if (last_kind & DLOOP_KIND_VECTOR) {
            blks[cur_blk].size = last_loop_p->el_size * last_loop_p->loop_params.v_t.blocksize;
            stride = last_loop_p->loop_params.v_t.stride;
        }
        /* DLOOP_KIND_CONTIG */
        else {
            blks[cur_blk].size = last_loop_p->el_size * last_loop_p->loop_params.c_t.count;
            stride = blks[cur_blk].size;
        }

        if ((cur_blk + 1) % last_loop_p->loop_params.count != 0) {
            cur_offset += stride;
        }
        /* last chunk in last level */
        else {
            int dp, ck;
            ck = cur_blk + 1;

            for (dp = max_depth; dp > 0; dp--) {
                DLOOP_Count loop_cnt = segment_ptr->stackelm[dp].loop_p->loop_params.count;
                if (ck % loop_cnt != 0)
                    break;
                ck = ck / loop_cnt;
            }

            if (segment_ptr->stackelm[dp].loop_p->kind & DLOOP_KIND_VECTOR) {
                cur_offset += (segment_ptr->stackelm[dp].loop_p->loop_params.v_t.stride +
                               blks[cur_blk].size - segment_ptr->stackelm[dp].loop_p->el_extent);
            }
            /* DLOOP_KIND_CONTIG */
            else {
                cur_offset += blks[cur_blk].size;
            }
        }

        if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE > 0) {
            cur_chunk_size += blks[cur_blk].size;
            if (cur_blk < nblks - 1 &&
                cur_chunk_size >= MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE) {
                cur_chunk_size = 0;
                blk_chunks[nchunks++] = cur_blk + 1;
            }
        }
        else if (cur_blk == nblks / 2) {
            blk_chunks[nchunks++] = cur_blk + 1;
        }
    } while (++cur_blk < nblks);

    MPIR_CHKPMEM_COMMIT();

    (*nblks_ptr) = nblks;
    (*blks_ptr) = blks;
    (*nchunks_ptr) = nchunks;
    (*blk_chunks_ptr) = blk_chunks;

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_GEN_DATATYPE_BLOCKS);
    if (segment_ptr != NULL)
        MPIDU_Segment_free(segment_ptr);

    return mpi_errno;

  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

/* called in CTS handler */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_start_send
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_start_send(MPIDI_VC_t * vc, MPIR_Request * req, MPL_IOV r_cookie)
{
    int mpi_errno = MPI_SUCCESS;
    MPI_Aint recv_size ATTRIBUTE((unused)), data_size = 0;
    int send_iscontig ATTRIBUTE((unused)), recv_iscontig ATTRIBUTE((unused));
    MPI_Aint send_true_lb = 0, send_true_extent ATTRIBUTE((unused));
    MPI_Aint recv_true_lb = 0, recv_true_extent ATTRIBUTE((unused));
    MPIDU_Datatype *send_dtptr, *recv_dtptr;
    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) req->ch.lmt_extpkt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);

    /* Received CTS only when parallel copy is initialized. */

    MPIDI_Datatype_get_info(lmt_extpkt->pcp.sender_count, lmt_extpkt->pcp.sender_dt,
                            send_iscontig, data_size, send_dtptr, send_true_lb);
    MPIDI_Datatype_get_info(lmt_extpkt->pcp.receiver_count, lmt_extpkt->pcp.receiver_dt,
                            recv_iscontig, recv_size, recv_dtptr, recv_true_lb);

    if (lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_CONTIG_CHUNKED) {
        /* Coordinate with the other side to copy contiguous chunks in parallel. */
        lmt_pip_copy_nchunked_contig(lmt_extpkt, data_size,
                                     ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                     ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb), "s");
    }
    else {
        /* Coordinate with the other side to copy noncontig chunks in parallel. */
        lmt_pip_copy_nchunked_noncontig(lmt_extpkt, data_size,
                                        ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                        ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb),
                                        "s");
    }

    /* No clean up here, release only after received DONE.
     * Because the receiver might still be accessing. */

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
    MPID_nem_lmt_pip_pcp_noncontig_block_t *noncontig_blks = NULL;
    int nblks = 0, nchunks = 0, *blk_chunks = NULL;

    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) rreq->ch.lmt_extpkt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);

#ifdef LMT_PIP_DBG
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
#endif

    MPIDI_Datatype_get_info(lmt_extpkt->pcp.sender_count, lmt_extpkt->pcp.sender_dt,
                            send_iscontig, data_size, send_dtptr, send_true_lb);
    MPIDI_Datatype_get_info(rreq->dev.user_count, rreq->dev.datatype,
                            recv_iscontig, recv_size, recv_dtptr, recv_true_lb);

    lmt_extpkt->pcp.nchunks = 0;
    lmt_extpkt->pcp.nblocks = 0;
    lmt_extpkt->pcp.noncontig_blocks = NULL;
    lmt_extpkt->pcp.chunk_size = 0;
    lmt_extpkt->pcp.block_chunks = NULL;

    /* Enable parallel copy only for contig or supported noncontig datatypes. */
    lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_DISABLED;

    if (send_iscontig && recv_iscontig) {
        if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD > 0 &&
            data_size >= MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD)
            lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_CONTIG_CHUNKED;
    }
    else if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_THRESHOLD > 0 &&
             data_size >= MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_THRESHOLD) {

        if (recv_iscontig) {
            /* Generate noncontig blocks from send buffer. */
            lmt_pip_gen_datatype_blocks((void *) lmt_extpkt->pcp.sender_buf,
                                        lmt_extpkt->pcp.sender_count,
                                        lmt_extpkt->pcp.sender_dt, &nblks, &noncontig_blks,
                                        &nchunks, &blk_chunks);
            if (nchunks > 0 && blk_chunks != NULL)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED;
        }
        else if (send_iscontig) {
            /* Generate noncontig blocks from receive buffer. */
            lmt_pip_gen_datatype_blocks(rreq->dev.user_buf, rreq->dev.user_count,
                                        rreq->dev.datatype, &nblks, &noncontig_blks,
                                        &nchunks, &blk_chunks);
            if (nchunks > 0 && blk_chunks != NULL)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED;
        }
        /* If noncontig on both sides, check if user passes symmetric hint. */
        else if (rreq->comm->dev.is_symm_datatype == TRUE) {
            /* Generate noncontig blocks from receive buffer, used in send buffer as well. */
            lmt_pip_gen_datatype_blocks(rreq->dev.user_buf, rreq->dev.user_count,
                                        rreq->dev.datatype, &nblks, &noncontig_blks,
                                        &nchunks, &blk_chunks);
            if (nchunks > 0 && blk_chunks != NULL)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED;
        }
    }

    /* Single copy. */
    if (lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_DISABLED) {

        PIP_DBG_PRINT("[%d] start single-copy, extpkt %p, data_size=%ld, sender_count=%ld, "
                      "send_iscontig=%d, receiver_count=%ld, recv_iscontig=%d\n",
                      myrank, lmt_extpkt, data_size, lmt_extpkt->pcp.sender_count,
                      send_iscontig, rreq->dev.user_count, recv_iscontig);

        mpi_errno = MPIR_Localcopy((const void *) lmt_extpkt->pcp.sender_buf,
                                   lmt_extpkt->pcp.sender_count, lmt_extpkt->pcp.sender_dt,
                                   rreq->dev.user_buf, rreq->dev.user_count, rreq->dev.datatype);
        if (mpi_errno)
            MPIR_ERR_POP(mpi_errno);
    }
    /* Parallel copy. */
    else {
        /* Set-up common receive information */
        lmt_extpkt->pcp.receiver_buf = (uintptr_t) rreq->dev.user_buf;
        lmt_extpkt->pcp.receiver_count = rreq->dev.user_count;
        lmt_extpkt->pcp.receiver_dt = rreq->dev.datatype;

        if (send_iscontig && recv_iscontig) {
            MPIR_Assert(lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_CONTIG_CHUNKED);

            /* Decide chunks by predefined chunk size. */
            if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_CHUNKSIZE > 0 &&
                MPIR_CVAR_NEMESIS_LMT_PIP_PCP_CHUNKSIZE < data_size) {
                lmt_extpkt->pcp.chunk_size = MPIR_CVAR_NEMESIS_LMT_PIP_PCP_CHUNKSIZE;
                lmt_extpkt->pcp.nchunks = data_size / lmt_extpkt->pcp.chunk_size;
                if (data_size % lmt_extpkt->pcp.chunk_size)
                    lmt_extpkt->pcp.nchunks++;
            }
            /* Always divide into two chunks. */
            else {
                lmt_extpkt->pcp.nchunks = 2;
                lmt_extpkt->pcp.chunk_size = data_size / 2;
            }

            /* Sync with sender to initial parallel copy. */
            OPA_store_int(&lmt_extpkt->pcp.complete_cnt, lmt_extpkt->pcp.nchunks);
            OPA_write_barrier();
            MPID_nem_lmt_send_CTS(vc, rreq, NULL, 0);

            /* Coordinate with the other side to copy contiguous chunks in parallel. */
            lmt_pip_copy_nchunked_contig(lmt_extpkt, data_size,
                                         ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                         ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb),
                                         "r");
        }
        else {
            MPIR_Assert(lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED ||
                        lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED ||
                        lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED);

            /* TODO: add option by using MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKEXTENT */

            /* Sync with sender to initial parallel copy. */
            lmt_extpkt->pcp.nchunks = nchunks;
            lmt_extpkt->pcp.block_chunks = blk_chunks;
            lmt_extpkt->pcp.noncontig_blocks = noncontig_blks;
            lmt_extpkt->pcp.nblocks = nblks;
            OPA_store_int(&lmt_extpkt->pcp.complete_cnt, lmt_extpkt->pcp.nchunks);

            OPA_write_barrier();
            MPID_nem_lmt_send_CTS(vc, rreq, NULL, 0);

            PIP_DBG_PRINT
                ("[%d] parallel-copy(r) start noncontig parallel copy, nchunks=%d, nblocks=%d, type=%d\n",
                 myrank, lmt_extpkt->pcp.nchunks, lmt_extpkt->pcp.nblocks, lmt_extpkt->pcp.type);

            /* Coordinate with the other side to copy noncontig chunks in parallel. */
            lmt_pip_copy_nchunked_noncontig(lmt_extpkt, data_size,
                                            ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                            ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb),
                                            "r");
        }

        /* Wait till all chunks are DONE. */
        while (OPA_load_int(&lmt_extpkt->pcp.complete_cnt) > 0);
        PIP_DBG_PRINT("[%d] parallel-copy(r) DONE\n", myrank);

        /* Note lmt_extpkt->pcp might already be freed by sender. */
        PIP_DBG_PRINT("[%d] parallel-copy(r): free noncontig_blks=%p, blk_chunks=%p\n", myrank,
                      noncontig_blks, blk_chunks);
        if (noncontig_blks)
            MPL_free(noncontig_blks);
        if (blk_chunks)
            MPL_free(blk_chunks);
    }

    /* DONE, notify sender.
     * Note that it is needed also in parallel copy, because we need ensure
     * sender can safely release lmt_extpkt.  */

    OPA_write_barrier();
    MPID_nem_lmt_send_DONE(vc, rreq);
    PIP_DBG_PRINT("[%d] issue single-copy DONE, data_size=%ld\n", myrank, data_size);

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

    OPA_read_barrier();

    /* Complete send request. */
    PIP_DBG_PRINT("[%d] %s: complete sreq %p/0x%x, free extpkt=%p, complete_cnt=%d\n",
                  myrank, __FUNCTION__, sreq, sreq->handle, sreq->ch.lmt_extpkt,
                  OPA_load_int(&((MPID_nem_pkt_lmt_rts_pipext_t *) sreq->ch.lmt_extpkt)->pcp.
                               complete_cnt));

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
