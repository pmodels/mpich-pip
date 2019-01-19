#ifndef PIP_REDUCE_INCLUDED
#define PIP_REDUCE_INCLUDED


void MPIR_create_shared_addr(MPIR_Comm *comm);
#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_reduce)
static inline int MPIDI_PIP_mpi_reduce(const void *sendbuf, void *recvbuf, int count,
                                       MPI_Datatype datatype, MPI_Op op, int root,
                                       MPIR_Comm * comm, MPIR_Errflag_t * errflag,
                                       const void *ch4_algo_parameters_container_in __attribute__((unused)))
{

	int mpi_errno = MPI_SUCCESS;
	int myrank = comm->rank;
	int psize = comm->local_size;
	int errLine;
	long long data_addr;
	void *dest = NULL;
	void *src = NULL;
	size_t dataSz, typesize;

	if (count == 0)
		goto fn_exit;

	/* Should not happen in application call */
	if (comm->shared_addr == NULL)
		MPIR_create_shared_addr(comm);
	// pip_barrier_t barp;
	// if(myrank == 0)
	// 	pip_barrier_init(barp, psize);

	typesize = MPIR_Datatype_get_basic_size(datatype);
	dataSz = typesize * count;


	/* Attach destination buffer located on root process */
	if (myrank == root) {
		// 	*(__s64*)(destheader + 1) = 1L;
		// 	*(__s64*)(destheader + 1) = 0L;
#ifndef NO_PIP_REDUCE_LOCAL
		if (sendbuf != MPI_IN_PLACE) {
			memcpy(recvbuf, sendbuf, dataSz);
		}
#endif
		data_addr = (long long)recvbuf;
		// printf("Rank: %d, root %d expose dest buffer with handler %llX\n", myrank, root, destheader.dtHandler);
		// fflush(stdout);
	} else {
		data_addr = (long long)sendbuf;
	}

	size_t sindex = (size_t) (myrank * count / psize);
	size_t ssize = sindex * typesize;
	size_t len = (size_t) ((myrank + 1) * count / psize) - sindex;
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	// mpi_errno = MPIDI_POSIX_mpi_allgather(&data_addr, 1, MPI_LONG_LONG, data_addr_array, 1, MPI_LONG_LONG, comm, errflag, NULL);
	// if (mpi_errno != MPI_SUCCESS) {
	// 	errLine = __LINE__;
	// 	goto fn_fail;
	// }
	// COLL_SHMEM_MODULE = PIP_MODULE;
	// printf("Reduce: Start pip_get_addr rank %d, pip_array %p\n", myrank, pip_array);
	// fflush(stdout);
	// sleep(30);
	// pip_get_addr(1, "pip_addr_array", &data_addr_array);

	// printf("Reduce: Begin assign addr rank %d, shared_addr %p, comm %p\n", myrank, comm->shared_addr, comm);
	// fflush(stdout);
	// long long *data_addr_array = data_array;
	comm->shared_addr[myrank] = data_addr;
	// printf("Reduce: complete assign addr rank %d, comm %p\n", myrank, comm);
	// fflush(stdout);
	// printf("myrank %d, before barrier\n", myrank);
	// fflush(stdout);
	// pip_barrier_wait(barp);
	// printf("myrank %d, after barrier\n", myrank);
	// fflush(stdout);
	// COLL_SHMEM_MODULE = POSIX_MODULE;

	MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	// COLL_SHMEM_MODULE = PIP_MODULE;
	void *outdest = (void*) ((char*) comm->shared_addr[root] + ssize);

	// printf("Reduce: ready copy rank %d\n", myrank);
	// fflush(stdout);
#ifndef NO_PIP_REDUCE_LOCAL
	/* Attach src data from each other within a for loop */
	for (int i = 0; i < psize; ++i) {
		if (i == root) {
			continue;
		}

		// if (myrank == i) {
		// 	src = (void*) sendbuf;
		// }

		// COLL_SHMEM_MODULE = POSIX_MODULE;
		// mpi_errno = MPIDI_POSIX_mpi_bcast(&src, 1, MPI_LONG_LONG, i, comm, errflag, NULL);
		// COLL_SHMEM_MODULE = PIP_MODULE;
		// if (mpi_errno != MPI_SUCCESS) {
		// 	errLine = __LINE__;
		// 	goto fn_fail;
		// }
		// printf("rank: %d, I get srcheader.dtHandler: %p\n", myrank, srcHeader.dtHandler);
		// fflush(stdout);

		// printf("In for loop: Rank = %d value = %d, i = %d\n", myrank, ((int*)srcdataBuf)[0], i);
		// fflush(stdout);
		/*
			Perform reduce computation
			The cache optimization can be applied in the future
		*/

		src = (void*) comm->shared_addr[i];
		void *insrc = (void*) ((char*) src + ssize);

		MPIR_Reduce_local(insrc, outdest, len, datatype, op);

	}
#endif
	// pip_barrier_wait(barp);
	// MPI_free(data_addr_array);
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	// COLL_SHMEM_MODULE = PIP_MODULE;

fn_exit :
	return mpi_errno;
fn_fail :
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	goto fn_exit;
}


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_reduce)
static inline int MPIDI_PIP_mpi_tree_based_reduce(const void *sendbuf, void *recvbuf, int count,
        MPI_Datatype datatype, MPI_Op op, int root,
        MPIR_Comm * comm, MPIR_Errflag_t * errflag,
        const void *ch4_algo_parameters_container_in __attribute__((unused)))
{
	/* Current tree base algorithm only deal with power of 2 sockets */
	int mpi_errno = MPI_SUCCESS;
	int myrank = comm->rank;
	int psize = comm->local_size;
	int errLine;
	void *dest = NULL;
	void *src = NULL;
	int step = 1;
	int new_rank = myrank;
	MPIR_Request *request;
	int size = MPIR_Datatype_get_basic_size(datatype) * count;

	void* local_buffer;
	int ack;

	if ((myrank & 1) == 0) {
		if (myrank != root) {
			local_buffer = MPL_malloc(size, MPL_MEM_OTHER);
			MPIR_Memcpy(local_buffer, sendbuf, size);
		} else {
			/* root sendbuf should be MPI_IN_PLACE */
			local_buffer = recvbuf;
		}
	} else {
		local_buffer = sendbuf;
	}

	while (1) {
		if (new_rank & 1) {
			src = local_buffer;
			mpi_errno = MPIDI_POSIX_mpi_send(&src, 1, MPI_LONG_LONG, myrank - step, 0, comm, MPIR_CONTEXT_INTRA_COLL, NULL, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			mpi_errno = MPID_PIP_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;

			mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, myrank - step, 0, comm, MPIR_CONTEXT_INTRA_COLL, MPI_STATUS_IGNORE, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			MPID_PIP_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			break;
		} else {
			mpi_errno = MPIDI_POSIX_mpi_recv(&src, 1, MPI_LONG_LONG, myrank + step, 0, comm, MPIR_CONTEXT_INTRA_COLL, MPI_STATUS_IGNORE, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			MPID_PIP_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
#ifndef NO_PIP_REDUCE_LOCAL
			MPIR_Reduce_local(src, local_buffer, count, datatype, op);
#endif

			mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, myrank + step, 0, comm, MPIR_CONTEXT_INTRA_COLL, NULL, &request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;
			MPID_PIP_Wait(request);
			if (unlikely(mpi_errno != MPI_SUCCESS))
				goto fn_fail;

			new_rank = new_rank >> 1;
			step = step << 1;
			if (step >= psize)
				break;
		}
	}

	if ((myrank & 1) == 0 && myrank != root) {
		MPL_free(local_buffer);
	}

fn_exit :
	return mpi_errno;
fn_fail :
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	goto fn_exit;
}

#endif