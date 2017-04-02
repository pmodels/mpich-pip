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
        ignore this option (Unused).

    - name        : MPIR_CVAR_NEMESIS_LMT_PIP_REMOTE_COMPLETE
      category    : NEMESIS
      type        : boolean
      default     : false
      class       : none
      verbosity   : MPI_T_VERBOSITY_USER_BASIC
      scope       : MPI_T_SCOPE_ALL_EQ
      description : >-
        If true, remote complete send request in PIP LMT implementation instead
        of issuing DONE packet.

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

#ifdef LMT_PIP_PROFILING
double lmt_pip_prof_unfold_datatype_timer = 0.0;
long lmt_pip_prof_lmt_unfold_datatype_cnt = 0;
double lmt_pip_prof_gen_chunk_timer = 0.0;
double lmt_pip_prof_dup_datatype_timer = 0.0;
long lmt_pip_prof_lmt_gen_chunk_cnt = 0;
long lmt_pip_prof_noncontig_nchunks[4] = { 0 }; /* as sender, as receiver; single(both noncontig); single(side noncontig); */
long lmt_pip_prof_contig_nchunks[3] = { 0 };    /* as sender, as receiver; single; */

long lmt_pip_prof_lmt_noncontig_cnt = 0;
#endif

void MPID_nem_lmt_pip_free_dtseg(const MPI_Datatype datatype);
static inline void lmt_pip_seg_release(MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr);
static inline void lmt_pip_try_remote_complete_req(MPIR_Request * req, int *completed);
static void lmt_pip_complete_sreq_cb(MPIR_Request * sreq);

#define LMT_PIP_SEG_VET(seg) (seg)->dt.vec

/* ----------------------------------------
 *  Datatype-segment cache routines
 * ---------------------------------------- */
typedef struct lmt_pip_datatype_blks_elt {
    MPL_UT_hash_handle hh;
    MPI_Datatype datatype;
    MPID_nem_lmt_pip_pcp_seg_t *seg;
} lmt_pip_datatype_seg_elt_t;

static lmt_pip_datatype_seg_elt_t *lmt_pip_cached_dtsegs = NULL;

/* Free datatype cache when such datatype is freed. */
void MPID_nem_lmt_pip_free_dtseg(const MPI_Datatype datatype)
{
    lmt_pip_datatype_seg_elt_t *dtseg = NULL;

    MPIR_FUNC_TERSE_STATE_DECL(MPID_NEM_LMT_PIP_FREE_DTSEG);
    MPIR_FUNC_TERSE_ENTER(MPID_NEM_LMT_PIP_FREE_DTSEG);

    if (lmt_pip_cached_dtsegs) {
        MPL_HASH_FIND(hh, lmt_pip_cached_dtsegs, &datatype, sizeof(MPI_Datatype), dtseg);
        if (dtseg != NULL) {
            PIP_DBG_PRINT("[%d] freed dtseg datatype 0x%lx, seg=%p, offset=%p\n",
                          myrank, (unsigned long) datatype, dtseg->seg,
                          LMT_PIP_SEG_VET(dtseg->seg).offsets);

            MPL_HASH_DEL(lmt_pip_cached_dtsegs, dtseg);
            lmt_pip_seg_release(&dtseg->seg);
            MPL_free(dtseg);
        }
    }
    MPIR_FUNC_TERSE_EXIT(MPID_NEM_LMT_PIP_FREE_DTSEG);
}

/* Destroy all datatype caches at finalize. */
static inline int lmt_pip_destroy_dtsegs(void *ignore)
{
    int mpi_errno = MPI_SUCCESS;
    lmt_pip_datatype_seg_elt_t *dtseg = NULL, *tmp = NULL;

    MPIR_FUNC_TERSE_STATE_DECL(LMT_PIP_DESTROY_DTSEGS);
    MPIR_FUNC_TERSE_ENTER(LMT_PIP_DESTROY_DTSEGS);

    PIP_DBG_PRINT("[%d] dtsegs_destroy start\n", myrank);

    if (lmt_pip_cached_dtsegs) {
        MPL_HASH_ITER(hh, lmt_pip_cached_dtsegs, dtseg, tmp) {
            PIP_DBG_PRINT("[%d] dtsegs_destroy: freed dtseg datatype 0x%lx, seg=%p, offset=%p\n",
                          myrank, (unsigned long) dtseg->datatype, dtseg->seg,
                          LMT_PIP_SEG_VET(dtseg->seg).offsets);
            MPL_HASH_DEL(lmt_pip_cached_dtsegs, dtseg);
            lmt_pip_seg_release(&dtseg->seg);
            MPL_free(dtseg);
        }
    }

    MPIR_FUNC_TERSE_EXIT(LMT_PIP_DESTROY_DTSEGS);
    return mpi_errno;
}

/* The datatype blocks is stored in a uthash, with datatype handle as key and
 * the unfolded blocks responsible as the value. */
static inline int lmt_pip_cache_dtseg(const MPI_Datatype datatype, MPID_nem_lmt_pip_pcp_seg_t * seg)
{
    int mpi_errno = MPI_SUCCESS;
    lmt_pip_datatype_seg_elt_t *dtseg = NULL;
    MPIR_FUNC_TERSE_STATE_DECL(LMT_PIP_CACHE_DTSEG);
    MPIR_FUNC_TERSE_ENTER(LMT_PIP_CACHE_DTSEG);

    if (lmt_pip_cached_dtsegs == NULL) {
        MPIR_Add_finalize(lmt_pip_destroy_dtsegs, NULL, MPIR_FINALIZE_CALLBACK_PRIO - 1);
    }

    dtseg = MPL_malloc(sizeof(lmt_pip_datatype_seg_elt_t));
    dtseg->datatype = datatype;
    dtseg->seg = seg;

    PIP_DBG_PRINT("[%d] cache dtseg datatype=0x%lx, seg=%p\n", myrank, (unsigned long) datatype,
                  seg);
    MPL_HASH_ADD(hh, lmt_pip_cached_dtsegs, datatype, sizeof(MPI_Datatype), dtseg);

    MPIR_FUNC_TERSE_EXIT(LMT_PIP_CACHE_DTSEG);
    return mpi_errno;
}

static inline void lmt_pip_find_dtseg(const MPI_Datatype datatype,
                                      MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr)
{
    lmt_pip_datatype_seg_elt_t *dtseg = NULL;

    MPIR_FUNC_TERSE_STATE_DECL(LMT_PIP_FIND_DTSEG);
    MPIR_FUNC_TERSE_ENTER(LMT_PIP_FIND_DTSEG);

    MPL_HASH_FIND(hh, lmt_pip_cached_dtsegs, &datatype, sizeof(MPI_Datatype), dtseg);
    if (dtseg) {
        PIP_DBG_PRINT("[%d] found cached dtseg datatype=0x%lx, seg=%p, nblocks=%d\n",
                      myrank, (unsigned long) datatype, dtseg->seg,
                      LMT_PIP_SEG_VET(dtseg->seg).nblocks);
        (*seg_ptr) = dtseg->seg;
    }
    MPIR_FUNC_TERSE_EXIT(LMT_PIP_FIND_DTSEG);
}

/* ----------------------------------------
 *  Parallel copy segment routines
 * ---------------------------------------- */

/* Internal Ref_count routine without adding into MPIR_Object */
#define lmt_pip_seg_set_ref(objptr_,val)                 \
    do {                                                 \
        OPA_store_int(&(objptr_)->ref_count, val);       \
    } while (0)
#define lmt_pip_seg_add_ref(objptr_)                      \
    do {                                                  \
        OPA_incr_int(&((objptr_)->ref_count));            \
    } while (0)
#define lmt_pip_seg_release_ref(objptr_,inuse_ptr)                      \
    do {                                                                \
        int got_zero_ = OPA_decr_and_test_int(&((objptr_)->ref_count)); \
        *(inuse_ptr) = got_zero_ ? 0 : 1;                               \
    } while (0)

static inline void lmt_pip_seg_release(MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr)
{
    int inuse = 0;

    MPIR_FUNC_TERSE_STATE_DECL(LMT_PIP_SEG_RELEASE);
    MPIR_FUNC_TERSE_ENTER(LMT_PIP_SEG_RELEASE);

    /* Release segment object when ref_count becomes 0. */
    lmt_pip_seg_release_ref((*seg_ptr), &inuse);
    if (!inuse) {
        MPL_free(LMT_PIP_SEG_VET(*seg_ptr).offsets);
        MPL_free((*seg_ptr));
    }
    PIP_DBG_PRINT("[%d] seg_release: free seg=%p, inuse=%d\n", myrank, (*seg_ptr), inuse);
    MPIR_FUNC_TERSE_EXIT(LMT_PIP_SEG_RELEASE);
}

static inline int lmt_pip_seg_create(MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr, int nblocks)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_lmt_pip_pcp_seg_t *noncontig_seg = NULL;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_SEG_CREATE);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_SEG_CREATE);
    MPIR_CHKPMEM_DECL(2);

    /* Allocate new segment */
    MPIR_CHKPMEM_MALLOC(noncontig_seg, MPID_nem_lmt_pip_pcp_seg_t *,
                        sizeof(MPID_nem_lmt_pip_pcp_seg_t), mpi_errno, "lmt PIP noncontig seg");
    MPIR_CHKPMEM_MALLOC(LMT_PIP_SEG_VET(noncontig_seg).offsets, MPI_Aint *,
                        sizeof(MPI_Aint) * nblocks, mpi_errno, "lmt PIP noncontig seg offset");

    MPIR_CHKPMEM_COMMIT();

    lmt_pip_seg_set_ref(noncontig_seg, 1);
    LMT_PIP_SEG_VET(noncontig_seg).nblocks = nblocks;
    LMT_PIP_SEG_VET(noncontig_seg).el_size = 0;
    LMT_PIP_SEG_VET(noncontig_seg).blk_cnt = 0;

    (*seg_ptr) = noncontig_seg;
    PIP_DBG_PRINT("[%d] seg_create: created seg=%p\n", myrank, (*seg_ptr));

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_SEG_CREATE);
    return mpi_errno;

  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}


/* Unfold vector type datatype. For general reuse, this routine unfolds single
 * count of such datatype and caches it in dtseg. The noncontig_seg object is
 * freed at type free or finalize. */
static inline int lmt_pip_seg_unfold_vec(MPI_Datatype datatype,
                                         MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_lmt_pip_pcp_seg_t *datatype_seg = NULL;
    int nblks = 1, cur_blk = 0;
    MPI_Aint cur_offset = 0;
    struct DLOOP_Dataloop *last_loop_p = NULL;
    struct MPIDU_Segment *segment_ptr = NULL;
    int last_kind = 0;
    int depth = 0, max_depth = 0;
    MPI_Aint blksize = 0;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_SEG_UNFOLD_VEC);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_SEG_UNFOLD_VEC);

    segment_ptr = MPIDU_Segment_alloc();
    MPIR_ERR_CHKANDJUMP1((segment_ptr == NULL), mpi_errno, MPI_ERR_OTHER,
                         "**nomem", "**nomem %s", "MPIDU_Segment_alloc");
    MPIDU_Segment_init(NULL, 1, datatype, segment_ptr, 0);

    for (depth = 0; depth < DLOOP_MAX_DATATYPE_DEPTH; depth++) {
        int kind = segment_ptr->stackelm[depth].loop_p->kind;

        /* FIXME: no routine supports irregular structure. */
        if (!(kind & DLOOP_KIND_CONTIG) && !(kind & DLOOP_KIND_VECTOR)) {
            (*seg_ptr) = NULL;
            goto fn_exit;
        }

        nblks *= segment_ptr->stackelm[depth].loop_p->loop_params.count;
        if (kind & DLOOP_FINAL_MASK) {
            last_kind = kind;
            break;
        }
    }
    max_depth = depth;

    /* Create new noncontig_seg with ref_count 1 for usage in cache.  */
    mpi_errno = lmt_pip_seg_create(&datatype_seg, nblks);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    cur_blk = 0;
    cur_offset = 0;
    last_loop_p = segment_ptr->stackelm[max_depth].loop_p;

    /* Same single contig block in vector datatype, only offset can be different. */
    LMT_PIP_SEG_VET(datatype_seg).el_size = last_loop_p->el_size;
    if (last_kind & DLOOP_KIND_VECTOR) {
        LMT_PIP_SEG_VET(datatype_seg).blk_cnt = last_loop_p->loop_params.v_t.blocksize;
    }
    /* DLOOP_KIND_CONTIG */
    else {
        LMT_PIP_SEG_VET(datatype_seg).blk_cnt = last_loop_p->loop_params.c_t.count;
    }
    blksize = LMT_PIP_SEG_VET(datatype_seg).el_size * LMT_PIP_SEG_VET(datatype_seg).blk_cnt;

    do {
        DLOOP_Offset stride = 0;
        LMT_PIP_SEG_VET(datatype_seg).offsets[cur_blk] = cur_offset;

        if (last_kind & DLOOP_KIND_VECTOR) {
            stride = last_loop_p->loop_params.v_t.stride;
        }
        else {  /* DLOOP_KIND_CONTIG */
            stride = LMT_PIP_SEG_VET(datatype_seg).el_size * LMT_PIP_SEG_VET(datatype_seg).blk_cnt;
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
                               blksize - segment_ptr->stackelm[dp].loop_p->el_extent);
            }
            /* DLOOP_KIND_CONTIG */
            else {
                cur_offset += blksize;
            }
        }
    } while (++cur_blk < nblks);

    /* Cache for reuse */
    mpi_errno = lmt_pip_cache_dtseg(datatype, datatype_seg);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    if (segment_ptr != NULL)
        MPIDU_Segment_free(segment_ptr);

    (*seg_ptr) = datatype_seg;

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_SEG_UNFOLD_VEC);
    return mpi_errno;

  fn_fail:
    goto fn_exit;
}

static inline int lmt_pip_seg_unfold_datatype(MPI_Datatype datatype,
                                              MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_lmt_pip_pcp_seg_t *cached_seg = NULL;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_SEG_UNFOLD_DATATYPE);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_SEG_UNFOLD_DATATYPE);

#ifdef LMT_PIP_PROFILING
    double lmt_pip_prof_unfold_datatype_timer_sta = MPI_Wtime();
#endif

    (*seg_ptr) = NULL;

    /* Check cached unfolded datatype segment. */
    lmt_pip_find_dtseg(datatype, &cached_seg);

    /* If not found, unfold datatype here. */
    if (cached_seg == NULL) {
        mpi_errno = lmt_pip_seg_unfold_vec(datatype, &cached_seg);

        /* Return with NULL seg if error or unsupported datatype */
        if (cached_seg == NULL || mpi_errno != MPI_SUCCESS) {
            MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_SEG_UNFOLD_DATATYPE);
            return mpi_errno;
        }
    }

    (*seg_ptr) = cached_seg;

#ifdef LMT_PIP_PROFILING
    lmt_pip_prof_unfold_datatype_timer += (MPI_Wtime() - lmt_pip_prof_unfold_datatype_timer_sta);
    lmt_pip_prof_lmt_unfold_datatype_cnt++;
#endif

    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_SEG_UNFOLD_DATATYPE);
    return mpi_errno;
}

static inline int lmt_pip_gen_noncontig_chunks(MPI_Aint count, MPI_Datatype datatype,
                                               MPI_Aint data_size,
                                               MPID_nem_lmt_pip_pcp_seg_t ** seg_ptr,
                                               int *nchunks_ptr, int **blk_chunks_ptr)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_lmt_pip_pcp_seg_t *dt_seg = NULL, *seg = NULL;
    int cur_blk = 0;
    MPI_Aint datatype_extent = 0;
    int nchunks = 0, *blk_chunks = NULL;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_GEN_NONCONTIG_CHUNKS);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_GEN_NONCONTIG_CHUNKS);
    MPIR_CHKPMEM_DECL(1);

    /* To ensure general re-usability, cache datatype rather than datatype * counts. */
    mpi_errno = lmt_pip_seg_unfold_datatype(datatype, &dt_seg);
    if (mpi_errno != MPI_SUCCESS)
        goto fn_fail;

    /* Unsupported datatypes */
    if (dt_seg == NULL) {
        (*seg_ptr) = NULL;
        (*nchunks_ptr) = 0;
        (*blk_chunks_ptr) = NULL;
        goto fn_exit;
    }

#ifdef LMT_PIP_PROFILING
    double lmt_pip_prof_gen_chunk_timer_sta = MPI_Wtime();
#endif

    if (count > 1) {
        int i, j;

        /* Allocate new segment */
        mpi_errno = lmt_pip_seg_create(&seg, LMT_PIP_SEG_VET(dt_seg).nblocks * count);
        if (mpi_errno != MPI_SUCCESS)
            goto fn_fail;

        LMT_PIP_SEG_VET(seg).blk_cnt = LMT_PIP_SEG_VET(dt_seg).blk_cnt;
        LMT_PIP_SEG_VET(seg).el_size = LMT_PIP_SEG_VET(dt_seg).el_size;

        /* Copy first count' offset */
        MPIR_Memcpy(LMT_PIP_SEG_VET(seg).offsets, LMT_PIP_SEG_VET(dt_seg).offsets,
                    sizeof(MPI_Aint) * LMT_PIP_SEG_VET(dt_seg).nblocks);

        /* Update offsets in later counts by adding datatype extent */
        MPIDU_Datatype_get_extent_macro(datatype, datatype_extent);
        for (i = 1; i < count; i++) {
            MPI_Aint data_off = datatype_extent * i;
            for (j = 0; j < LMT_PIP_SEG_VET(dt_seg).nblocks; j++) {
                LMT_PIP_SEG_VET(seg).offsets[i * LMT_PIP_SEG_VET(dt_seg).nblocks + j] =
                    LMT_PIP_SEG_VET(dt_seg).offsets[j] + data_off;
            }
        }
    }
    else {
        /* Use the cached segment (count == 1), increase ref_count */
        lmt_pip_seg_add_ref(dt_seg);
        seg = dt_seg;
    }

    /* Decide parallel copy chunks by size and blocks */
    {
        MPI_Aint blk_sz;
        int nblks_in_chunk;

        MPIR_CHKPMEM_MALLOC(blk_chunks, int *, sizeof(int) * LMT_PIP_SEG_VET(seg).nblocks,
                            mpi_errno, "lmt PIP blk_chunks");

        blk_sz = LMT_PIP_SEG_VET(seg).el_size * LMT_PIP_SEG_VET(seg).blk_cnt;
        nblks_in_chunk = MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE / blk_sz;
        if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE % blk_sz)
            nblks_in_chunk++;

        nchunks = 0;
        cur_blk = 0;
        /* Divide chunks based on predefined chunksize if data_size is large enough. */
        if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE > 0 &&
            data_size > MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKSIZE) {
            do {
                blk_chunks[nchunks++] = cur_blk;
                cur_blk += nblks_in_chunk;
            } while (cur_blk < LMT_PIP_SEG_VET(seg).nblocks);
        }
        /* Otherwise simply divide by two */
        else {
            blk_chunks[nchunks++] = 0;
            blk_chunks[nchunks++] = LMT_PIP_SEG_VET(seg).nblocks / 2 + 1;
        }
    }

    MPIR_CHKPMEM_COMMIT();

    (*seg_ptr) = seg;
    (*nchunks_ptr) = nchunks;
    (*blk_chunks_ptr) = blk_chunks;

#ifdef LMT_PIP_PROFILING
    lmt_pip_prof_gen_chunk_timer += (MPI_Wtime() - lmt_pip_prof_gen_chunk_timer_sta);
    lmt_pip_prof_lmt_gen_chunk_cnt++;
#endif

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_GEN_NONCONTIG_CHUNKS);
    return mpi_errno;

  fn_fail:
    MPIR_CHKPMEM_REAP();
    goto fn_exit;
}

/* ----------------------------------------
 *  Internal parallel copy routines
 * ---------------------------------------- */

/* Checked address alignment (copied from veccpy.h). */
#define LMT_PIP_ALIGN8_TEST(p0,p1) ((((MPI_Aint)(uintptr_t) p0 | (MPI_Aint)(uintptr_t) p1) & 0x7) == 0)
#define LMT_PIP_ALIGN4_TEST(p0,p1) ((((MPI_Aint)(uintptr_t) p0 | (MPI_Aint)(uintptr_t) p1) & 0x3) == 0)
#define LMT_PIP_SHORT_COPY_CNT (16)

static inline void lmt_pip_block_copy(void *rbuf_ptr, void *sbuf_ptr, int blk_cnt, MPI_Aint el_size)
{
    int i;

    /* Try aligned data move for short data */
    if (blk_cnt <= LMT_PIP_SHORT_COPY_CNT) {
        if (el_size == 8 && LMT_PIP_ALIGN8_TEST(sbuf_ptr, rbuf_ptr)) {
            int64_t *dest = (int64_t *) rbuf_ptr, *src = (int64_t *) sbuf_ptr;
            for (i = 0; i < blk_cnt; i++)
                dest[i] = src[i];
            return;
        }
        else if (el_size == 4 && LMT_PIP_ALIGN4_TEST(sbuf_ptr, rbuf_ptr)) {
            int32_t *dest = (int32_t *) rbuf_ptr, *src = (int32_t *) sbuf_ptr;
            for (i = 0; i < blk_cnt; i++)
                dest[i] = src[i];
            return;
        }
        else if (el_size == 2) {
            int16_t *dest = (int16_t *) rbuf_ptr, *src = (int16_t *) sbuf_ptr;
            for (i = 0; i < blk_cnt; i++)
                dest[i] = src[i];
            return;
        }
    }

    MPIR_Memcpy(rbuf_ptr, sbuf_ptr, blk_cnt * el_size);
}

static inline void lmt_pip_copy_nchunked_contig(MPID_nem_pkt_lmt_rts_pipext_t * lmt_extpkt,
                                                MPI_Aint data_size, char *sbuf, char *rbuf,
                                                const char *dbg_nm
#ifdef LMT_PIP_PROFILING
                                                , int prof_sr   /* s:0, r:1 */
#endif
)
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

#ifdef LMT_PIP_PROFILING
        lmt_pip_prof_contig_nchunks[prof_sr]++;
#endif
    }

    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_COPY_NCHUNKED_CONTIG);
}

static inline void lmt_pip_copy_symmetric_vec(MPID_nem_lmt_pip_pcp_seg_t * seg,
                                              int blk_sta, int blk_end, char *sbuf, char *rbuf)
{
    char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
    int cur_blk = 0;

    for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
        sbuf_ptr = sbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];
        rbuf_ptr = rbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];

        lmt_pip_block_copy(rbuf_ptr, sbuf_ptr, LMT_PIP_SEG_VET(seg).blk_cnt,
                           LMT_PIP_SEG_VET(seg).el_size);
    }
}

static inline void lmt_pip_copy_nchunked_noncontig(MPID_nem_pkt_lmt_rts_pipext_t * lmt_extpkt,
                                                   MPI_Aint data_size ATTRIBUTE((unused)),
                                                   char *sbuf, char *rbuf, const char *dbg_nm
#ifdef LMT_PIP_PROFILING
                                                   , int prof_sr        /* s:0, r:1 */
#endif
)
{
    char *sbuf_ptr = NULL, *rbuf_ptr = NULL;
    MPI_Aint copy_size = 0, contig_offset = 0;
    int cur_chunk = 0, copied = 0;
    int cur_blk = 0, blk_sta = 0, blk_end = 0;
    MPID_nem_lmt_pip_pcp_seg_t *seg = lmt_extpkt->pcp.noncontig_seg;

    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_COPY_NCHUNKED_NONCONTIG);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_COPY_NCHUNKED_NONCONTIG);

    cur_chunk = OPA_fetch_and_incr_int(&lmt_extpkt->pcp.offset);
    while (cur_chunk < lmt_extpkt->pcp.nchunks) {

        /* Calculate block range */
        blk_sta = lmt_extpkt->pcp.block_chunks[cur_chunk];
        if (cur_chunk == lmt_extpkt->pcp.nchunks - 1) {
            /* FIXME: in receiver chunked case, the sender datasize can be smaller
             * than receiver. Thus should not always set blk_end to the last block
             * on receiver. Need fix in gen_noncontig_chunks. */
            blk_end = LMT_PIP_SEG_VET(seg).nblocks - 1;
        }
        else {
            blk_end = lmt_extpkt->pcp.block_chunks[cur_chunk + 1] - 1;
        }

        switch (lmt_extpkt->pcp.type) {
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED:
            {
                copy_size = LMT_PIP_SEG_VET(seg).blk_cnt * LMT_PIP_SEG_VET(seg).el_size;

                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    contig_offset = copy_size * cur_blk;
                    sbuf_ptr = sbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];
                    rbuf_ptr = rbuf + contig_offset;

                    lmt_pip_block_copy(rbuf_ptr, sbuf_ptr, LMT_PIP_SEG_VET(seg).blk_cnt,
                                       LMT_PIP_SEG_VET(seg).el_size);
                }
            }
            break;
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED:
            {
                copy_size = LMT_PIP_SEG_VET(seg).blk_cnt * LMT_PIP_SEG_VET(seg).el_size;

                PIP_DBG_PRINT
                    ("[%d] parallel-copy(%s): copying recv noncontig, seg=%p, nchunks=%d/%d, nblocks=(%d - %d)/%d, type=%d,"
                     "sbuf=%p + off %ld, rbuf %p + off %ld, copy_size=%ld(%ld)\n", myrank, dbg_nm,
                     cur_chunk, lmt_extpkt->pcp.nchunks, seg, blk_sta, blk_end,
                     LMT_PIP_SEG_VET(seg).nblocks, lmt_extpkt->pcp.type, sbuf, copy_size * blk_sta,
                     rbuf, LMT_PIP_SEG_VET(seg).offsets[blk_sta],
                     copy_size * (blk_end - blk_sta + 1), copy_size);

                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    contig_offset = copy_size * cur_blk;
                    sbuf_ptr = sbuf + contig_offset;
                    rbuf_ptr = rbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];

                    PIP_DBG_PRINT
                        ("[%d] parallel-copy(%s): copying recv noncontig, nchunks=%d/%d, cur_blk=%d/(%d-%d),"
                         "sbuf_ptr=%p (%ld), rbuf_ptr=%p (%ld), copy_size=%ld*%ld\n",
                         myrank, dbg_nm, cur_chunk, lmt_extpkt->pcp.nchunks, cur_blk, blk_sta,
                         blk_end, sbuf_ptr, contig_offset, rbuf_ptr,
                         LMT_PIP_SEG_VET(seg).offsets[cur_blk], LMT_PIP_SEG_VET(seg).blk_cnt,
                         LMT_PIP_SEG_VET(seg).el_size);

                    lmt_pip_block_copy(rbuf_ptr, sbuf_ptr, LMT_PIP_SEG_VET(seg).blk_cnt,
                                       LMT_PIP_SEG_VET(seg).el_size);
                }
            }
            break;
        case MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED:
            {
                for (cur_blk = blk_sta; cur_blk <= blk_end; cur_blk++) {
                    copy_size = LMT_PIP_SEG_VET(seg).blk_cnt * LMT_PIP_SEG_VET(seg).el_size;
                    sbuf_ptr = sbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];
                    rbuf_ptr = rbuf + LMT_PIP_SEG_VET(seg).offsets[cur_blk];

                    lmt_pip_block_copy(rbuf_ptr, sbuf_ptr, LMT_PIP_SEG_VET(seg).blk_cnt,
                                       LMT_PIP_SEG_VET(seg).el_size);
                }

                PIP_DBG_PRINT
                    ("[%d] parallel-copy(%s): copying noncontig, nchunks=%d/%d, nblocks=(%d - %d)/%d, type=%d,"
                     "offset=%ld, copy_size=%ld(%ld)\n", myrank, dbg_nm, cur_chunk,
                     lmt_extpkt->pcp.nchunks, blk_sta, blk_end,
                     LMT_PIP_SEG_VET(seg).nblocks, lmt_extpkt->pcp.type,
                     LMT_PIP_SEG_VET(seg).offsets[cur_blk],
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

#ifdef LMT_PIP_PROFILING
        lmt_pip_prof_noncontig_nchunks[prof_sr] += (blk_end - blk_sta + 1);
#endif

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

/* ----------------------------------------
 *  Public PIP LMT routines
 * ---------------------------------------- */

/* called in MPID_nem_lmt_RndvSend */
#undef FUNCNAME
#define FUNCNAME MPID_nem_lmt_pip_initiate_lmt
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_nem_lmt_pip_initiate_lmt(MPIDI_VC_t * vc, MPIDI_CH3_Pkt_t * pkt, MPIR_Request * req)
{
    int mpi_errno = MPI_SUCCESS;
    MPID_nem_pkt_lmt_rts_t *const rts_pkt = (MPID_nem_pkt_lmt_rts_t *) pkt;
    int send_dt_replaced ATTRIBUTE((unused)) = 0 ;

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
    rts_pkt->extpkt->pcp.sender_count = req->dev.user_count;
    rts_pkt->extpkt->pcp.sender_dt = req->dev.datatype;
    rts_pkt->extpkt->pcp.send_dtptr = NULL;

    if (!MPIR_DATATYPE_IS_PREDEFINED(req->dev.datatype)) {
        MPIDU_Datatype *send_dtptr = NULL;
        MPIDU_Datatype_get_ptr(req->dev.datatype, send_dtptr);

        /*  Replace with basic datatype */
        if (send_dtptr->is_contig) {
            rts_pkt->extpkt->pcp.sender_dt = send_dtptr->basic_type;
            send_dt_replaced = 1;
            rts_pkt->extpkt->pcp.sender_count =
                (send_dtptr->size / MPIDU_Datatype_get_basic_size(send_dtptr->basic_type)
                 * rts_pkt->extpkt->pcp.sender_count);
        }
        else {
            /* The receiver side will duplicate a datatype */
            rts_pkt->extpkt->pcp.send_dtptr = send_dtptr;
        }
    }

    OPA_store_int(&rts_pkt->extpkt->pcp.offset, 0);
    OPA_store_int(&rts_pkt->extpkt->pcp.complete_cnt, 0);

    req->ch.lmt_extpkt = rts_pkt->extpkt;       /* store in request, thus can free it
                                                 * when LMT done.*/

    if (MPIR_CVAR_NEMESIS_LMT_PIP_REMOTE_COMPLETE && MPID_Request_remote_cc_enabled(req)) {
        rts_pkt->extpkt->sreq = req;
        req->dev.remote_completed_cb = lmt_pip_complete_sreq_cb;

        MPID_Rcc_req_progress_register(req);    /* register to progress engine. */
    }

    OPA_write_barrier();

    MPID_nem_lmt_send_RTS(vc, rts_pkt, NULL, 0);

    PIP_DBG_PRINT
        ("[%d] %s: issued RTS: extpkt %p, sbuf[0x%lx,%ld, sender_dt 0x%lx, (send_dtptr=%p), (replaced %d), sz %ld], sreq 0x%lx, remote_cc_enabled %d\n",
         myrank, __FUNCTION__, rts_pkt->extpkt, rts_pkt->extpkt->pcp.sender_buf,
         rts_pkt->extpkt->pcp.sender_count, (unsigned long) rts_pkt->extpkt->pcp.sender_dt,
         rts_pkt->extpkt->pcp.send_dtptr, send_dt_replaced, rts_pkt->data_sz,
         (unsigned long) rts_pkt->sender_req_id, MPID_Request_remote_cc_enabled(req));

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
    MPI_Aint recv_size ATTRIBUTE((unused)), data_size = 0;
    int send_iscontig ATTRIBUTE((unused)), recv_iscontig ATTRIBUTE((unused));
    MPI_Aint send_true_lb = 0;
    MPI_Aint recv_true_lb = 0;
    MPIDU_Datatype *send_dtptr, *recv_dtptr;
    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) req->ch.lmt_extpkt;
    MPI_Datatype sender_dt = (MPI_Datatype) lmt_extpkt->pcp.sender_dt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);

    /* Received CTS only when parallel copy is initialized. */

    MPIDI_Datatype_get_info(lmt_extpkt->pcp.sender_count, sender_dt,
                            send_iscontig, data_size, send_dtptr, send_true_lb);

    /* FIXME: workaround for fortran special datatypes */
    if (lmt_extpkt->pcp.receiver_dtptr != NULL) {
        recv_true_lb = lmt_extpkt->pcp.receiver_dtptr->true_lb;
    }
    else {
        MPI_Datatype receiver_dt = (MPI_Datatype) lmt_extpkt->pcp.receiver_dt;
        MPIDI_Datatype_get_info(lmt_extpkt->pcp.receiver_count, receiver_dt,
                                recv_iscontig, recv_size, recv_dtptr, recv_true_lb);
    }

    PIP_DBG_PRINT
        ("[%d] parallel-copy(s) start parallel copy, sbuf=0x%lx (lb=%ld), rbuf=0x%lx (lb=%ld), count=%ld, "
         "receiver_dt=0x%lx (receiver_dtptr=%p), nchunks=%d, type=%d\n",
         myrank, lmt_extpkt->pcp.sender_buf, send_true_lb,
         lmt_extpkt->pcp.receiver_buf, recv_true_lb, lmt_extpkt->pcp.receiver_count,
         lmt_extpkt->pcp.receiver_dt, lmt_extpkt->pcp.receiver_dtptr, lmt_extpkt->pcp.nchunks,
         lmt_extpkt->pcp.type);

    if (lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_CONTIG_CHUNKED) {
        /* Coordinate with the other side to copy contiguous chunks in parallel. */
        lmt_pip_copy_nchunked_contig(lmt_extpkt, data_size,
                                     ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                     ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb), "s"
#ifdef LMT_PIP_PROFILING
                                     , 0
#endif
);
    }
    else {
        /* Coordinate with the other side to copy noncontig chunks in parallel. */
        lmt_pip_copy_nchunked_noncontig(lmt_extpkt, data_size,
                                        ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                        ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb), "s"
#ifdef LMT_PIP_PROFILING
                                        , 0
#endif
);
    }

#ifdef LMT_PIP_PROFILING
    lmt_pip_prof_lmt_noncontig_cnt++;
#endif

    /* No clean up here, release only after received DONE.
     * Because the receiver might still be accessing. */

    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_START_SEND);
    return mpi_errno;
}


static int lmt_pip_datatype_dup(MPIDU_Datatype * old_dtp, MPI_Datatype * newtype)
{
    int mpi_errno = MPI_SUCCESS;
    MPIDU_Datatype *new_dtp = 0;

    /* allocate new datatype object and handle */
    new_dtp = (MPIDU_Datatype *) MPIR_Handle_obj_alloc(&MPIDU_Datatype_mem);
    if (!new_dtp) {
        /* --BEGIN ERROR HANDLING-- */
        mpi_errno = MPIR_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                         "MPIDU_Type_dup", __LINE__, MPI_ERR_OTHER, "**nomem", 0);
        goto fn_fail;
        /* --END ERROR HANDLING-- */
    }

    /* fill in datatype */
    MPIR_Object_set_ref(new_dtp, 1);
    /* new_dtp->handle is filled in by MPIR_Handle_obj_alloc() */
    new_dtp->is_contig = old_dtp->is_contig;
    new_dtp->size = old_dtp->size;
    new_dtp->extent = old_dtp->extent;
    new_dtp->ub = old_dtp->ub;
    new_dtp->lb = old_dtp->lb;
    new_dtp->true_ub = old_dtp->true_ub;
    new_dtp->true_lb = old_dtp->true_lb;
    new_dtp->alignsize = old_dtp->alignsize;
    new_dtp->has_sticky_ub = old_dtp->has_sticky_ub;
    new_dtp->has_sticky_lb = old_dtp->has_sticky_lb;
    new_dtp->is_permanent = old_dtp->is_permanent;
    new_dtp->is_committed = old_dtp->is_committed;

    new_dtp->attributes = NULL; /* Attributes are copied in the
                                 * top-level MPI_Type_dup routine */
    new_dtp->cache_id = -1;     /* ??? */
    new_dtp->name[0] = 0;       /* The Object name is not copied on a dup */
    new_dtp->n_builtin_elements = old_dtp->n_builtin_elements;
    new_dtp->builtin_element_size = old_dtp->builtin_element_size;
    new_dtp->basic_type = old_dtp->basic_type;

    new_dtp->dataloop = NULL;
    new_dtp->dataloop_size = old_dtp->dataloop_size;
    new_dtp->dataloop_depth = old_dtp->dataloop_depth;
    new_dtp->hetero_dloop = NULL;
    new_dtp->hetero_dloop_size = old_dtp->hetero_dloop_size;
    new_dtp->hetero_dloop_depth = old_dtp->hetero_dloop_depth;
    *newtype = new_dtp->handle;

    if (old_dtp->is_committed) {
        MPIR_Assert(old_dtp->dataloop != NULL);
        MPIDU_Dataloop_dup(old_dtp->dataloop, old_dtp->dataloop_size, &new_dtp->dataloop);
        if (old_dtp->hetero_dloop != NULL) {
            /* at this time MPI_COMPLEX doesn't have this loop...
             * -- RBR, 02/01/2007
             */
            MPIDU_Dataloop_dup(old_dtp->hetero_dloop,
                               old_dtp->hetero_dloop_size, &new_dtp->hetero_dloop);
        }

#ifdef MPID_Type_commit_hook
        MPID_Type_commit_hook(new_dtp);
#endif /* MPID_Type_commit_hook */
    }

  fn_fail:
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
    MPI_Aint recv_size ATTRIBUTE((unused)), data_size = 0;
    int send_iscontig = 0, recv_iscontig = 0;
    MPI_Aint send_true_lb = 0;
    MPI_Aint recv_true_lb = 0;
    MPIDU_Datatype *send_dtptr, *recv_dtptr;
    MPID_nem_lmt_pip_pcp_seg_t *noncontig_seg = NULL;
    int nchunks = 0, *blk_chunks = NULL;
    int remote_completed = 0;

    MPID_nem_pkt_lmt_rts_pipext_t *lmt_extpkt =
        (MPID_nem_pkt_lmt_rts_pipext_t *) rreq->ch.lmt_extpkt;
    MPI_Datatype sender_dt = lmt_extpkt->pcp.sender_dt;

    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);
    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_NEM_LMT_PIP_START_RECV);

#ifdef LMT_PIP_DBG
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
#endif

    PIP_DBG_PRINT("[%d] start_recv start, lmt_extpkt=%p, send_dt=0x%lx, recv_d=0d%lx\n",
                  myrank, lmt_extpkt, lmt_extpkt->pcp.sender_dt, rreq->dev.datatype);

    /* FIXME: workaround for fortran special datatypes */
#ifdef LMT_PIP_PROFILING
    double lmt_pip_prof_dup_datatype_timer_sta = MPI_Wtime();
#endif
    MPI_Datatype sender_dt_dup = MPI_DATATYPE_NULL;
    if (lmt_extpkt->pcp.send_dtptr != NULL) {
        MPIR_Assert(lmt_extpkt->pcp.send_dtptr->is_contig == 0);

        lmt_pip_datatype_dup(lmt_extpkt->pcp.send_dtptr, &sender_dt_dup);
        PIP_DBG_PRINT("[%d] %s: replace sender_dt 0x%lx -> send_dt_dup=0x%lx\n",
                      myrank, __FUNCTION__, sender_dt, sender_dt_dup);
        sender_dt = sender_dt_dup;
    }
#ifdef LMT_PIP_PROFILING
    lmt_pip_prof_dup_datatype_timer += (MPI_Wtime() - lmt_pip_prof_dup_datatype_timer_sta);
#endif

    MPIDI_Datatype_get_info(lmt_extpkt->pcp.sender_count, sender_dt,
                            send_iscontig, data_size, send_dtptr, send_true_lb);
    MPIDI_Datatype_get_info(rreq->dev.user_count, rreq->dev.datatype,
                            recv_iscontig, recv_size, recv_dtptr, recv_true_lb);

    lmt_extpkt->pcp.nchunks = 0;
    /* Used in contig copy */
    lmt_extpkt->pcp.chunk_size = 0;
    /* Used in noncontig copy */
    lmt_extpkt->pcp.noncontig_seg = NULL;
    lmt_extpkt->pcp.block_chunks = NULL;

    /* Enable parallel copy only for contig or supported noncontig datatypes. */
    lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_DISABLED;

    if (send_iscontig && recv_iscontig) {
        if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD > 0 &&
            data_size >= MPIR_CVAR_NEMESIS_LMT_PIP_PCP_THRESHOLD)
            lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_CONTIG_CHUNKED;
    }
    else if (MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_THRESHOLD > 0 &&
             data_size > MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_THRESHOLD) {

        if (recv_iscontig) {
            /* Generate noncontig blocks based on sender buffer. */
            lmt_pip_gen_noncontig_chunks(lmt_extpkt->pcp.sender_count,
                                         sender_dt, data_size,
                                         &noncontig_seg, &nchunks, &blk_chunks);
            if (nchunks > 0)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_SENDER_CHUNKED;
        }
        else if (send_iscontig) {
            /* Generate noncontig blocks based on receive buffer. */
            lmt_pip_gen_noncontig_chunks(rreq->dev.user_count, rreq->dev.datatype,
                                         data_size, &noncontig_seg, &nchunks, &blk_chunks);
            if (nchunks > 0)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_RECEIVER_CHUNKED;
        }
        /* If noncontig on both sides, check if user passes symmetric hint
         * (same datatype, count can be different). */
        else if (rreq->comm->dev.is_symm_datatype == TRUE) {
            /* Generate noncontig blocks based on sender buffer. */
            lmt_pip_gen_noncontig_chunks(lmt_extpkt->pcp.sender_count, sender_dt,
                                         data_size, &noncontig_seg, &nchunks, &blk_chunks);
            if (nchunks > 0)
                lmt_extpkt->pcp.type = MPID_NEM_LMT_PIP_PCP_NONCONTIG_SYMM_CHUNKED;
        }
    }

    /* Single copy. */
    if (lmt_extpkt->pcp.type == MPID_NEM_LMT_PIP_PCP_DISABLED) {
        int copied = 0;

        /* Try direct copy for noncontig to noncontig case. Localcopy uses pack-unpack. */
        if (!send_iscontig && !recv_iscontig && rreq->comm->dev.is_symm_datatype == TRUE) {
            MPID_nem_lmt_pip_pcp_seg_t *seg;
            MPI_Aint datatype_extent;
            int i;

            mpi_errno = lmt_pip_seg_unfold_datatype(sender_dt, &seg);
            if (mpi_errno != MPI_SUCCESS)
                goto fn_fail;

            if (seg != NULL) {
                char *sbuf_ptr = (char *) lmt_extpkt->pcp.sender_buf + send_true_lb;
                char *rbuf_ptr = (char *) rreq->dev.user_buf + recv_true_lb;
                MPIDU_Datatype_get_extent_macro(sender_dt, datatype_extent);
                for (i = 0; i < lmt_extpkt->pcp.sender_count; i++) {
                    MPI_Aint data_off = datatype_extent * i;

                    PIP_DBG_PRINT
                        ("[%d] start symm-noncontig single-copy, data_off=%ld(count %d), "
                         "sbuf_ptr=%p, rbuf_ptr=%p, nblocks=%d\n",
                         myrank, data_off, i, sbuf_ptr, rbuf_ptr, LMT_PIP_SEG_VET(seg).nblocks - 1);

                    lmt_pip_copy_symmetric_vec(seg, 0, LMT_PIP_SEG_VET(seg).nblocks - 1,
                                               (sbuf_ptr + data_off), (rbuf_ptr + data_off));
                }
                copied = 1;
            }
            /* For any unsupported datatypes, simply do pack-unpack. */
        }

        if (!copied) {
            PIP_DBG_PRINT
                ("[%d] start single-copy, extpkt %p, data_size=%ld, sender_dt=0x%lx, sender_count=%ld, "
                 "send_iscontig=%d, receiver_count=%ld, recv_iscontig=%d\n", myrank, lmt_extpkt,
                 data_size, (unsigned long) sender_dt, lmt_extpkt->pcp.sender_count, send_iscontig,
                 rreq->dev.user_count, recv_iscontig);

            mpi_errno = MPIR_Localcopy((const void *) lmt_extpkt->pcp.sender_buf,
                                       lmt_extpkt->pcp.sender_count, sender_dt,
                                       rreq->dev.user_buf, rreq->dev.user_count,
                                       rreq->dev.datatype);
            if (mpi_errno)
                MPIR_ERR_POP(mpi_errno);

#ifdef LMT_PIP_PROFILING
            if (send_iscontig && recv_iscontig) {
                lmt_pip_prof_contig_nchunks[2]++;
            }
            else if (!send_iscontig && !recv_iscontig) {
                lmt_pip_prof_noncontig_nchunks[2]++;
            }
            else {
                lmt_pip_prof_noncontig_nchunks[3]++;
            }
#endif
        }

        /* First try fast remote complete.
         * Do not use in parallel copy because sender accesses sreq at CTS arrival
         * but that sreq might already be completed or freed. */
        if (MPIR_CVAR_NEMESIS_LMT_PIP_REMOTE_COMPLETE && lmt_extpkt->sreq) {
            OPA_write_barrier();
            lmt_pip_try_remote_complete_req(lmt_extpkt->sreq, &remote_completed);
            PIP_DBG_PRINT("[%d] remote COMPLETE send request %p (%d), data_size=%ld\n",
                          myrank, lmt_extpkt->sreq, remote_completed, data_size);
        }
    }
    /* Parallel copy. */
    else {
        /* Set-up common receive information */
        lmt_extpkt->pcp.receiver_buf = (uintptr_t) rreq->dev.user_buf;
        lmt_extpkt->pcp.receiver_count = rreq->dev.user_count;
        lmt_extpkt->pcp.receiver_dt = rreq->dev.datatype;
        lmt_extpkt->pcp.receiver_dtptr = NULL;

        if (!MPIR_DATATYPE_IS_PREDEFINED(rreq->dev.datatype)) {
            MPIDU_Datatype_get_ptr(rreq->dev.datatype, lmt_extpkt->pcp.receiver_dtptr);
        }

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

            PIP_DBG_PRINT
                ("[%d] parallel-copy(r) start contig parallel copy, sbuf=0x%lx(lb=%ld), rbuf=0x%lx(lb=%ld), count=%ld, "
                 "receiver_dt=0x%lx (receiver_dtptr=%p), nchunks=%d, nblocks=%d, type=%d\n",
                 myrank, lmt_extpkt->pcp.sender_buf, send_true_lb,
                 lmt_extpkt->pcp.receiver_buf, recv_true_lb, lmt_extpkt->pcp.receiver_count,
                 lmt_extpkt->pcp.receiver_dt, lmt_extpkt->pcp.receiver_dtptr,
                 lmt_extpkt->pcp.nchunks);

            /* Sync with sender to initial parallel copy. */
            OPA_store_int(&lmt_extpkt->pcp.complete_cnt, lmt_extpkt->pcp.nchunks);
            OPA_write_barrier();
            MPID_nem_lmt_send_CTS(vc, rreq, NULL, 0);

            /* Coordinate with the other side to copy contiguous chunks in parallel. */
            lmt_pip_copy_nchunked_contig(lmt_extpkt, data_size,
                                         ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                         ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb), "r"
#ifdef LMT_PIP_PROFILING
                                         , 1
#endif
);
        }
        else {
            /* TODO: add option by using MPIR_CVAR_NEMESIS_LMT_PIP_PCP_NONCONTIG_CHUNKEXTENT */

            /* Sync with sender to initial parallel copy. */
            lmt_extpkt->pcp.nchunks = nchunks;
            lmt_extpkt->pcp.block_chunks = blk_chunks;
            lmt_extpkt->pcp.noncontig_seg = noncontig_seg;
            OPA_store_int(&lmt_extpkt->pcp.complete_cnt, lmt_extpkt->pcp.nchunks);

            OPA_write_barrier();
            MPID_nem_lmt_send_CTS(vc, rreq, NULL, 0);

            PIP_DBG_PRINT
                ("[%d] parallel-copy(r) start noncontig parallel copy, sbuf=0x%lx(lb=%ld), rbuf=0x%lx(lb=%ld, true_lb=%ld), count=%ld, "
                 "receiver_dt=0x%lx (receiver_dtptr=%p), nchunks=%d, nblocks=%d, type=%d\n",
                 myrank, lmt_extpkt->pcp.sender_buf, send_true_lb,
                 lmt_extpkt->pcp.receiver_buf, recv_true_lb,
                 lmt_extpkt->pcp.receiver_dtptr->true_lb, lmt_extpkt->pcp.receiver_count,
                 lmt_extpkt->pcp.receiver_dt, lmt_extpkt->pcp.receiver_dtptr,
                 lmt_extpkt->pcp.nchunks, LMT_PIP_SEG_VET(lmt_extpkt->pcp.noncontig_seg).nblocks,
                 lmt_extpkt->pcp.type);

            /* Coordinate with the other side to copy noncontig chunks in parallel. */
            lmt_pip_copy_nchunked_noncontig(lmt_extpkt, data_size,
                                            ((char *) lmt_extpkt->pcp.sender_buf + send_true_lb),
                                            ((char *) lmt_extpkt->pcp.receiver_buf + recv_true_lb),
                                            "r"
#ifdef LMT_PIP_PROFILING
                                            , 1
#endif
);

#ifdef LMT_PIP_PROFILING
            lmt_pip_prof_lmt_noncontig_cnt++;
#endif
        }

        /* Wait till all chunks are DONE. */
        while (OPA_load_int(&lmt_extpkt->pcp.complete_cnt) > 0);
        PIP_DBG_PRINT("[%d] parallel-copy(r) DONE\n", myrank);
    }

    /* Note lmt_extpkt->pcp might already be freed by sender.
     * So we free object through local variables. */
    if (blk_chunks)
        MPL_free(blk_chunks);
    if (noncontig_seg)
        lmt_pip_seg_release(&noncontig_seg);
    /* FIXME: workaround for fortran special datatypes */
    if (sender_dt_dup != MPI_DATATYPE_NULL)
        MPIR_Type_free_impl(&sender_dt_dup);

    /* DONE, notify sender.
     * Note that it is needed also in parallel copy, because we need ensure
     * sender can safely release lmt_extpkt.  */

    /* If remote compelte is not allowed, do regual DONE packet. */
    if (!remote_completed) {
        OPA_write_barrier();
        MPID_nem_lmt_send_DONE(vc, rreq);
        PIP_DBG_PRINT("[%d] issue copy DONE, data_size=%ld\n", myrank, data_size);
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


static void lmt_pip_complete_sreq(MPIR_Request * sreq)
{
    OPA_read_barrier();

    MPIR_Assert(sreq->ch.lmt_extpkt);
    MPL_free(sreq->ch.lmt_extpkt);
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

    PIP_DBG_PRINT("[%d] %s: complete sreq %p/0x%x, free extpkt=%p, complete_cnt=%d, "
                  "sreq->remote_cc=%d, cc=%d\n",
                  myrank, __FUNCTION__, sreq, sreq->handle, sreq->ch.lmt_extpkt,
                  OPA_load_int(&((MPID_nem_pkt_lmt_rts_pipext_t *) sreq->ch.lmt_extpkt)->
                               pcp.complete_cnt), OPA_load_int(&sreq->dev.remote_cc),
                  MPIR_cc_get(sreq->cc));

    /* Complete send request. */
    lmt_pip_complete_sreq(sreq);

    mpi_errno = MPID_Request_complete(sreq);
    if (mpi_errno != MPI_SUCCESS)
        MPIR_ERR_POP(mpi_errno);

  fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_NEM_LMT_PIP_DONE_SEND);
    return mpi_errno;
  fn_fail:
    goto fn_exit;
}

static void lmt_pip_complete_sreq_cb(MPIR_Request * sreq)
{
    MPIR_FUNC_VERBOSE_STATE_DECL(LMT_PIP_COMPLETE_SREQ_CB);
    MPIR_FUNC_VERBOSE_ENTER(LMT_PIP_COMPLETE_SREQ_CB);

    PIP_DBG_PRINT
        ("[%d] %s: complete sreq %p, remote_cc=%d, cc=%d, free extpkt=%p, complete_cnt=%d, " "\n",
         myrank, __FUNCTION__, sreq, OPA_load_int(&sreq->dev.remote_cc), MPIR_cc_get(sreq->cc),
         sreq->ch.lmt_extpkt,
         OPA_load_int(&((MPID_nem_pkt_lmt_rts_pipext_t *) sreq->ch.lmt_extpkt)->pcp.complete_cnt));

    lmt_pip_complete_sreq(sreq);

    MPIR_FUNC_VERBOSE_EXIT(LMT_PIP_COMPLETE_SREQ_CB);
}


static inline void lmt_pip_try_remote_complete_req(MPIR_Request * req, int *completed)
{
    (*completed) = 0;
    if (MPID_Request_remote_cc_enabled(req)) {
        OPA_decr_int(&req->dev.remote_cc);
        (*completed) = 1;
    }
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
