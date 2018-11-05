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
	// printf("Rank: %d completes bcast\n", myrank);
	// fflush(stdout);



	if (myrank != root) {
		mpi_errno = xpmemAttachMem(&destheader, &destdataBuf, &destrealBuf, &destapid);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		// printArray(myrank, destdataBuf, count);
		// printf("Rank: %d, attach dest buffer with handler %llX\n", myrank, destheader.dtHandler);
		// fflush(stdout);
	}
	// printf("Rank: %d, attach dest buffer with handler %llX, and %d\n", myrank, destheader.dtHandler, ((int*)destdataBuf)[0]);
	// fflush(stdout);

	/* Attach src data from each other within a for loop */
	for (int i = 0; i < psize; ++i) {
		if (i == root) {
			continue;
		}

		if (myrank == i) {
			srcdataBuf = (void*) sendbuf;
			mpi_errno = xpmemExposeMem(sendbuf, dataSz, &srcHeader);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
		}
		COLL_SHMEM_MODULE = POSIX_MODULE;
		mpi_errno = MPIDI_POSIX_mpi_bcast(&srcHeader, 4, MPI_LONG_LONG, i, comm, errflag, NULL);
		COLL_SHMEM_MODULE = XPMEM_MODULE;
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
		// printf("rank: %d, I get srcheader.dtHandler: %p\n", myrank, srcHeader.dtHandler);
		// fflush(stdout);
		if (myrank != i) {
			mpi_errno = xpmemAttachMem(&srcHeader, &srcdataBuf, &srcrealBuf, &srcapid);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}

			// printArray(myrank, srcdataBuf, count);
			// printf("rank: %d, I succeed in attaching mem srcdataBuf: %p\n", myrank, srcdataBuf);
			// fflush(stdout);
			// printf("rank: %d, test first value: %d\n", myrank, *(int*)srcdataBuf);
		}
		// printf("In for loop: Rank = %d value = %d, i = %d\n", myrank, ((int*)srcdataBuf)[0], i);
		// fflush(stdout);
		/*
			Perform reduce computation
			The cache optimization can be applied in the future
		*/
		size_t sindex = (size_t) (myrank * count / psize);
		size_t len = (size_t) ((myrank + 1) * count / psize) - sindex;

		void *insrc = (void*) ((char*) srcdataBuf + sindex * typesize);
		void *outdest = (void*) ((char*) destdataBuf + sindex * typesize);

		// int *p = (int*) insrc;
		// for (int i = 0; i < len; ++i) {
		// 	p[i]++;
		// }
		// printf("Rank: %d reduce local add, sindex: %d, len: %d\n", myrank, sindex, len);
		// fflush(stdout);
		// int *tsrc = (int*) insrc;
		// int *tout = (int*) outdest;
		// for (int i = 0; i < len; ++i) {
		// 	tout[i] += tsrc[i];
		// }
		MPIR_Reduce_local(insrc, outdest, len, datatype, op);

		if (myrank != i) {
			mpi_errno = xpmemDetachMem(srcrealBuf, &srcapid);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
		}
		// while (1);
		// printf("Rank: %d, detach memory complete\n", myrank);
		// fflush(stdout);
		/*
			Maybe I do not need barrier?
			Assure xpmem_remove can block until receiver processes release handler
		*/
		// while (1);
		COLL_SHMEM_MODULE = POSIX_MODULE;
		MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
		COLL_SHMEM_MODULE = XPMEM_MODULE;
		if (myrank == i) {
			mpi_errno = xpmemRemoveMem(&srcHeader);
			if (mpi_errno != MPI_SUCCESS) {
				errLine = __LINE__;
				goto fn_fail;
			}
		}
	}

	// if (myrank == root) {
	// 	printf("Final results: ");
	// 	for (int i = 0; i < count; ++i) {
	// 		printf("%d ", ((int*)recvbuf)[i]);
	// 	}
	// 	printf("\n");
	// 	fflush(stdout);
	// }
	if (myrank != root) {
		mpi_errno = xpmemDetachMem(destrealBuf, &destapid);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}
	COLL_SHMEM_MODULE = POSIX_MODULE;
	MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	COLL_SHMEM_MODULE = XPMEM_MODULE;
	if (myrank == root) {
		mpi_errno = xpmemRemoveMem(&destheader);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}

	// free(destheader);

	goto fn_exit;
fn_fail :
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit :
	return mpi_errno;
}

#endif