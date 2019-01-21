#ifndef PIP_BARRIER_INCLUDED
#define PIP_BARRIER_INCLUDED

#undef FUNCNAME
#define FUNCNAME MPIDI_PIP_mpi_barrier
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline int MPIDI_PIP_mpi_barrier(MPIR_Comm * comm)
{
	int mpi_errno = MPI_SUCCESS;

	MPIR_FUNC_VERBOSE_STATE_DECL(MPID_STATE_MPIDI_PIP_MPI_BARRIER);
	MPIR_FUNC_VERBOSE_ENTER(MPID_STATE_MPIDI_PIP_MPI_BARRIER);

	int flag = comm->current_flag;
	int myrank = comm->rank;
	int psize = comm->local_size;
	int i;
	comm->barrier_flags[flag][myrank] = 0;
	i = 0;
	do {
		for (; i < psize; ++i)
			if (comm->barrier_flags[flag][i] != 0){
				// printf("rank %d is not at the barrier\n", i);
				// fflush(stdout);
				break;
			}
	} while (i != psize);

	int init_flag = (flag + 2) % 3;
	comm->current_flag = (comm->current_flag + 1) % 3;
	comm->barrier_flags[init_flag][myrank] = 0xff;

	if (mpi_errno)
		MPIR_ERR_POP(mpi_errno);

	MPIR_FUNC_VERBOSE_EXIT(MPID_STATE_MPIDI_PIP_MPI_BARRIER);

fn_exit:
	return mpi_errno;
fn_fail:
	goto fn_exit;
}

#endif