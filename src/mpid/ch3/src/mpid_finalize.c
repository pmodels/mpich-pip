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
//#define LMT_PIP_PROFILING
#if defined(HAVE_PIP) && defined(LMT_PIP_PROFILING)
extern double lmt_pip_prof_unfold_datatype_timer;
extern long lmt_pip_prof_lmt_unfold_datatype_cnt;
extern double lmt_pip_prof_gen_chunk_timer;
extern double lmt_pip_prof_dup_datatype_timer;
extern long lmt_pip_prof_lmt_gen_chunk_cnt;
extern long lmt_pip_prof_noncontig_nchunks[4]; /* as sender, as receiver; single(both noncontig); single(side noncontig);*/
extern long lmt_pip_prof_lmt_noncontig_cnt;
extern long lmt_pip_prof_contig_nchunks[3]; /* as sender, as receiver; single; */
#endif

static inline void profiling_print(void) {
    int rank = 0, size = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

#if defined(HAVE_PIP) && defined(LMT_PIP_PROFILING)
    {
        double avg_unfold_datatype_timers[3] = {0.0};
        double avg_gen_chunk_timers[3] = {0.0};
        double avg_dup_datatype_timers[3] = {0.0};
        long avg_prof_contig_nchunks[9] = {0};
        long avg_prof_noncontig_nchunks[12] = {0};

        MPI_Reduce(&lmt_pip_prof_dup_datatype_timer, &avg_dup_datatype_timers[0], 1,
                   MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_dup_datatype_timer, &avg_dup_datatype_timers[1], 1,
                   MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_dup_datatype_timer, &avg_dup_datatype_timers[2], 1,
                   MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

        MPI_Reduce(&lmt_pip_prof_unfold_datatype_timer, &avg_unfold_datatype_timers[0], 1,
                   MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_unfold_datatype_timer, &avg_unfold_datatype_timers[1], 1,
                   MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_unfold_datatype_timer, &avg_unfold_datatype_timers[2], 1,
                   MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

        MPI_Reduce(&lmt_pip_prof_gen_chunk_timer, &avg_gen_chunk_timers[0], 1,
                   MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_gen_chunk_timer, &avg_gen_chunk_timers[1], 1,
                   MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(&lmt_pip_prof_gen_chunk_timer, &avg_gen_chunk_timers[2], 1,
                   MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

        MPI_Reduce(lmt_pip_prof_noncontig_nchunks, &avg_prof_noncontig_nchunks[0], 4,
                   MPI_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(lmt_pip_prof_noncontig_nchunks, &avg_prof_noncontig_nchunks[4], 4,
                MPI_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(lmt_pip_prof_noncontig_nchunks, &avg_prof_noncontig_nchunks[8], 4,
                MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

        MPI_Reduce(lmt_pip_prof_contig_nchunks, &avg_prof_contig_nchunks[0], 2,
                MPI_LONG, MPI_MIN, 0, MPI_COMM_WORLD);
        MPI_Reduce(lmt_pip_prof_contig_nchunks, &avg_prof_contig_nchunks[3], 2,
                MPI_LONG, MPI_MAX, 0, MPI_COMM_WORLD);
        MPI_Reduce(lmt_pip_prof_contig_nchunks, &avg_prof_contig_nchunks[6], 2,
                MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);

        if(rank == 0) {
            avg_dup_datatype_timers[2] = avg_dup_datatype_timers[2] / size;
            avg_unfold_datatype_timers[2] = avg_unfold_datatype_timers[2] / size;
            avg_gen_chunk_timers[2] = avg_gen_chunk_timers[2] / size;
            avg_prof_noncontig_nchunks[8] = avg_prof_noncontig_nchunks[8] / size;
            avg_prof_noncontig_nchunks[9] = avg_prof_noncontig_nchunks[9] / size;
            avg_prof_noncontig_nchunks[10] = avg_prof_noncontig_nchunks[10] / size;
            avg_prof_noncontig_nchunks[11] = avg_prof_noncontig_nchunks[11] / size;
            avg_prof_contig_nchunks[6] = avg_prof_contig_nchunks[6] / size;
            avg_prof_contig_nchunks[7] = avg_prof_contig_nchunks[7] / size;
            avg_prof_contig_nchunks[8] = avg_prof_contig_nchunks[8] / size;

            fprintf(stdout, "[%d] PIP_LMT_PROF: unfold_datatype_timer min %.4lf max %.4lf avg %.4lf, cnt %d\n",
                    rank, avg_unfold_datatype_timers[0], avg_unfold_datatype_timers[1],
                    avg_unfold_datatype_timers[2], lmt_pip_prof_lmt_unfold_datatype_cnt);
            fprintf(stdout, "[%d] PIP_LMT_PROF: gen_chunk_timer min %.4lf max %.4lf avg %.4lf, cnt %d\n",
                    rank, avg_gen_chunk_timers[0], avg_gen_chunk_timers[1],
                    avg_gen_chunk_timers[2], lmt_pip_prof_lmt_gen_chunk_cnt);
            fprintf(stdout, "[%d] PIP_LMT_PROF: dup_datatype_timer min %.4lf max %.4lf avg %.4lf\n",
                    rank, avg_dup_datatype_timers[0], avg_dup_datatype_timers[1],
                    avg_dup_datatype_timers[2]);
            fprintf(stdout, "[%d] PIP_LMT_PROF: noncontig_nchunks "
                    "(s) min %ld max %ld avg %ld, "
                    "(r) min %ld max %ld avg %ld, cnt %ld\n",
                    rank, avg_prof_noncontig_nchunks[0], avg_prof_noncontig_nchunks[4], avg_prof_noncontig_nchunks[8],
                    avg_prof_noncontig_nchunks[1], avg_prof_noncontig_nchunks[5], avg_prof_noncontig_nchunks[9],
                    lmt_pip_prof_lmt_noncontig_cnt);
            fprintf(stdout, "[%d] PIP_LMT_PROF: noncontig_single_cnts "
                    "(n2n) min %ld max %ld avg %ld, "
                    "(c2n) min %ld max %ld avg %ld\n", rank,
                    avg_prof_noncontig_nchunks[2], avg_prof_noncontig_nchunks[6], avg_prof_noncontig_nchunks[10],
                    avg_prof_noncontig_nchunks[3], avg_prof_noncontig_nchunks[7], avg_prof_noncontig_nchunks[11]);
            fprintf(stdout, "[%d] PIP_LMT_PROF: contig_nchunks "
                    "(s) min %ld max %ld avg %ld, "
                    "(r) min %ld max %ld avg %ld,"
                    "(single) min %ld max %ld avg %ld\n",
                    rank, avg_prof_contig_nchunks[0], avg_prof_contig_nchunks[3], avg_prof_contig_nchunks[6],
                    avg_prof_contig_nchunks[1], avg_prof_contig_nchunks[4], avg_prof_contig_nchunks[7],
                    avg_prof_contig_nchunks[2], avg_prof_contig_nchunks[5], avg_prof_contig_nchunks[8]);
        }
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
