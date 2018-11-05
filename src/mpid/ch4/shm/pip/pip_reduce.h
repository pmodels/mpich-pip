#ifndef PIP_REDUCE_INCLUDED
#define PIP_REDUCE_INCLUDED


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_reduce)
static inline int MPIDI_PIP_mpi_reduce(const void *sendbuf, void *recvbuf, int count,
                                       MPI_Datatype datatype, MPI_Op op, int root,
                                       MPIR_Comm * comm, MPIR_Errflag_t * errflag,
                                       const void *ch4_algo_parameters_container_in __attribute__((unused))) {
	/*
		Current version does not consider the inter sockets communications.
		optimized implementation should add this feature which needs to
		modify the comm structure and split function.
	*/

	int mpi_errno = MPI_SUCCESS;
	int myrank = comm->rank;
	int psize = comm->local_size;
	int errLine;
	void *dest = NULL;
	void *src = NULL;
	size_t dataSz, typesize;
	if (count == 0)
		goto fn_exit;

	typesize =  MPIR_Datatype_get_basic_size(datatype);
	dataSz = typesize * count;
	/* Attach destination buffer located on root process */
	if (myrank == root) {
		// 	*(__s64*)(destheader + 1) = 1L;
		// 	*(__s64*)(destheader + 1) = 0L;
		dest = recvbuf;

		if (sendbuf != MPI_IN_PLACE) {
			memcpy(recvbuf, sendbuf, dataSz);
		}
		// printf("Rank: %d, root %d expose dest buffer with handler %llX\n", myrank, root, destheader.dtHandler);
		// fflush(stdout);
	}
	COLL_SHMEM_MODULE = POSIX_MODULE;
	mpi_errno = MPIDI_POSIX_mpi_bcast(&dest, 1, MPI_LONG_LONG, root, comm, errflag, NULL);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	COLL_SHMEM_MODULE = PIP_MODULE;

	size_t sindex = (size_t) (myrank * count / psize);
	size_t ssize = sindex * typesize;
	size_t len = (size_t) ((myrank + 1) * count / psize) - sindex;
	void *outdest = (void*) ((char*) dest + ssize);

	/* Attach src data from each other within a for loop */
	for (int i = 0; i < psize; ++i) {
		if (i == root) {
			continue;
		}

		if (myrank == i) {
			src = (void*) sendbuf;
		}

		COLL_SHMEM_MODULE = POSIX_MODULE;
		mpi_errno = MPIDI_POSIX_mpi_bcast(&src, 1, MPI_LONG_LONG, i, comm, errflag, NULL);
		COLL_SHMEM_MODULE = PIP_MODULE;
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		// printf("rank: %d, I get srcheader.dtHandler: %p\n", myrank, srcHeader.dtHandler);
		// fflush(stdout);

		// printf("In for loop: Rank = %d value = %d, i = %d\n", myrank, ((int*)srcdataBuf)[0], i);
		// fflush(stdout);
		/*
			Perform reduce computation
			The cache optimization can be applied in the future
		*/


		void *insrc = (void*) ((char*) src + ssize);
		MPIR_Reduce_local(insrc, outdest, len, datatype, op);

	}
	COLL_SHMEM_MODULE = POSIX_MODULE;
	MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	COLL_SHMEM_MODULE = PIP_MODULE;

	goto fn_exit;
fn_fail :
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit :
	return mpi_errno;
}

#endif