/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "mpidimpl.h"

/* FIXME: This routine needs to be factored into finalize actions per module,
   In addition, we should consider registering callbacks for those actions
   rather than direct routine calls.
 */

#if defined(HAVE_PIP) && defined(LMT_PIP_PROFILING)
extern double lmt_pip_prof_unfold_datatype_timer;
extern int lmt_pip_prof_lmt_unfold_datatype_cnt;
extern double lmt_pip_prof_gen_chunk_timer;
extern int lmt_pip_prof_lmt_gen_chunk_cnt;
extern int lmt_pip_prof_noncontig_nchunks;
extern int lmt_pip_prof_copied_noncontig_nblks;
extern int lmt_pip_prof_lmt_noncontig_cnt;
#endif

static inline void profiling_print(void) {
    int rank = 0, size = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

#if defined(HAVE_PIP) && defined(LMT_PIP_PROFILING)
    {
        double avg_unfold_datatype_timer = 0.0;
        double avg_gen_chunk_timer = 0.0;
        int avg_noncontig_nchunks = 0, avg_copied_noncontig_nblks = 0;
        if (lmt_pip_prof_unfold_datatype_timer > 0)
            avg_unfold_datatype_timer = lmt_pip_prof_unfold_datatype_timer /
                lmt_pip_prof_lmt_unfold_datatype_cnt;
        if (lmt_pip_prof_gen_chunk_timer > 0)
            avg_gen_chunk_timer = lmt_pip_prof_gen_chunk_timer /
                lmt_pip_prof_lmt_gen_chunk_cnt;
        if (lmt_pip_prof_lmt_noncontig_cnt > 0) {
            avg_noncontig_nchunks = lmt_pip_prof_noncontig_nchunks /
                    lmt_pip_prof_lmt_noncontig_cnt;
            avg_copied_noncontig_nblks = lmt_pip_prof_copied_noncontig_nblks /
                    lmt_pip_prof_lmt_noncontig_cnt;
        }

        fprintf(stdout, "[%d] PIP_LMT_PROF: avg unfold_datatype_timer %.4lf, cnt %d\n",
                rank, avg_unfold_datatype_timer * 1000 * 1000, lmt_pip_prof_lmt_unfold_datatype_cnt);
        fprintf(stdout, "[%d] PIP_LMT_PROF: avg gen_chunk_timer %.4lf, cnt %d\n",
                rank, avg_gen_chunk_timer * 1000 * 1000, lmt_pip_prof_lmt_gen_chunk_cnt);
        fprintf(stdout, "[%d] PIP_LMT_PROF: avg noncontig_nchunks %d, copied_nblks %d, cnt %d\n",
                rank, avg_noncontig_nchunks, avg_copied_noncontig_nblks,
                lmt_pip_prof_lmt_noncontig_cnt);
    }
#endif


#ifdef MSG_SEND_PROFILING
    {
        int i;
        long msg_send_prof_cnts_min[MSG_SEND_PROFILING_MAX] = { 0 };
        long msg_send_prof_cnts_max[MSG_SEND_PROFILING_MAX] = { 0 };
        long msg_send_prof_cnts_avg[MSG_SEND_PROFILING_MAX] = { 0 };

        MPI_Reduce(msg_send_prof_cnts, msg_send_prof_cnts_min, MSG_SEND_PROFILING_MAX,
                   MPI_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(msg_send_prof_cnts, msg_send_prof_cnts_max, MSG_SEND_PROFILING_MAX,
                   MPI_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(msg_send_prof_cnts, msg_send_prof_cnts_avg, MSG_SEND_PROFILING_MAX,
                   MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

        if (rank == 0) {
            for (i = 0; i < MSG_SEND_PROFILING_MAX; i++) {
                msg_send_prof_cnts_avg[i] = msg_send_prof_cnts_avg[i] / size;
                fprintf(stdout, "[%d] MSG_SEND_PROF[%d]: min %ld, max %ld, avg %ld\n",
                        rank, i, msg_send_prof_cnts_min[i], msg_send_prof_cnts_max[i],
                        msg_send_prof_cnts_avg[i]);
            }
        }
    }
#endif

    fflush(stdout);
}

#undef FUNCNAME
#define FUNCNAME MPID_Finalize
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
int MPID_Finalize(void)
{
    int mpi_errno = MPI_SUCCESS;
    MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPID_FINALIZE);

    MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPID_FINALIZE);

    profiling_print();

    //#define AH
#ifdef AH
    {
        extern unsigned long long ah_yield_count;
        fprintf(stderr, "AHAHA ah_yield_count=%Lu\n", ah_yield_count);
    }
#endif

    /*
     * Wait for all posted receives to complete.  For now we are not doing 
     * this since it will cause invalid programs to hang.
     * The side effect of not waiting is that posted any source receives 
     * may erroneous blow up.
     *
     * For now, we are placing a warning at the end of MPID_Finalize() to 
     * inform the user if any outstanding posted receives exist.
     */
    /* FIXME: The correct action here is to begin a shutdown protocol
     * that lets other processes know that this process is now
     * in finalize.
     *
     * Note that only requests that have been freed with MPI_Request_free
     * are valid at this point; other pending receives can be ignored
     * since a valid program should wait or test for them before entering
     * finalize.
     *
     * The easist fix is to allow an MPI_Barrier over comm_world (and
     * any connected processes in the MPI-2 case).  Once the barrier
     * completes, all processes are in finalize and any remaining
     * unmatched receives will never be matched (by a correct program;
     * a program with a send in a separate thread that continues after
     * some thread calls MPI_Finalize is erroneous).
     *
     * Avoiding the barrier is hard.  Consider this sequence of steps:
     * Send in-finalize message to all connected processes.  Include
     * information on whether there are pending receives.
     *   (Note that a posted receive with any source is a problem)
     *   (If there are many connections, then this may take longer than
     *   the barrier)
     * Allow connection requests from anyone who has not previously
     * connected only if there is an possible outstanding receive;
     * reject others with a failure (causing the source process to
     * fail).
     * Respond to an in-finalize message with the number of posted receives
     * remaining.  If both processes have no remaining receives, they
     * can both close the connection.
     *
     * Processes with no pending receives and no connections can exit,
     * calling PMI_Finalize to let the process manager know that they
     * are in a controlled exit.
     *
     * Processes that still have open connections must then try to contact
     * the remaining processes.
     *
     * August 2010:
     *
     * The barrier has been removed so that finalize won't hang when
     * another processes has died.  This allows processes to finalize
     * and exit while other processes are still executing.  This has
     * the following consequences:
     *
     *  * If a process tries to send a message to a process that has
     *    exited before completing the matching receive, it will now
     *    get an error.  This is an erroneous program.
     *
     *  * If a process finalizes before completing a nonblocking
     *    send, the message might not be sent.  Similarly, if it
     *    finalizes before completing all receives, the sender may
     *    get an error.  These are erroneous programs.
     *
     *  * A process may isend to another process that has already
     *    terminated, then cancel the send.  The program is not
     *    erroneous in this case, but this will result in an error.
     *    This can be fixed by not returning an error until the app
     *    completes the send request.  If the app cancels the
     *    request, we need to to search the pending send queue and
     *    cancel it, in which case an error shouldn't be generated.
     */

    /* Re-enabling the close step because many tests are failing
     * without it, particularly under gforker */

    mpi_errno = MPIDI_Port_finalize();
    if (mpi_errno) {
        MPIR_ERR_POP(mpi_errno);
    }

#if 1
    /* FIXME: The close actions should use the same code as the other
     connection close code */
    mpi_errno = MPIDI_PG_Close_VCs();
    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);
    /*
     * Wait for all VCs to finish the close protocol
     */
    mpi_errno = MPIDI_CH3U_VC_WaitForClose();
    if (mpi_errno) {
        MPIR_ERR_POP(mpi_errno);
    }
#endif

    /* Note that the CH3I_Progress_finalize call has been removed; the
     CH3_Finalize routine should call it */
    mpi_errno = MPIDI_CH3_Finalize();
    if (mpi_errno) {
        MPIR_ERR_POP(mpi_errno);
    }

#ifdef MPID_NEEDS_ICOMM_WORLD
    mpi_errno = MPIR_Comm_release_always(MPIR_Process.icomm_world);
    if (mpi_errno) MPIR_ERR_POP(mpi_errno);
#endif

    mpi_errno = MPIR_Comm_release_always(MPIR_Process.comm_self);
    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    mpi_errno = MPIR_Comm_release_always(MPIR_Process.comm_world);
    if (mpi_errno)
        MPIR_ERR_POP(mpi_errno);

    /* Tell the process group code that we're done with the process groups.
     This will notify PMI (with PMI_Finalize) if necessary.  It
     also frees all PG structures, including the PG for COMM_WORLD, whose
     pointer is also saved in MPIDI_Process.my_pg */
    mpi_errno = MPIDI_PG_Finalize();
    if (mpi_errno) {
        MPIR_ERR_POP(mpi_errno);
    }

#ifndef MPIDI_CH3_HAS_NO_DYNAMIC_PROCESS
    MPIDI_CH3_FreeParentPort();
#endif

    /* Release any SRbuf pool storage */
    if (MPIDI_CH3U_SRBuf_pool) {
        MPIDI_CH3U_SRBuf_element_t *p, *pNext;
        p = MPIDI_CH3U_SRBuf_pool;
        while (p) {
            pNext = p->next;
            MPL_free(p);
            p = pNext;
        }
    }

    MPIDI_RMA_finalize();

    MPL_free(MPIDI_failed_procs_string);

    MPIDU_Ftb_finalize();

    fn_exit:
    MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPID_FINALIZE);
    return mpi_errno;
    fn_fail:
    goto fn_exit;
}
