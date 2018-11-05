#ifndef XPMEM_REDUCE_INCLUDED
#define XPMEM_REDUCE_INCLUDED

#include "shm.h"
#include "xpmem_progress.h"

static inline void printArray(int rank, int* array, int count) {
	printf("rank %d array: ", rank);
	for (int i = 0; i < count; ++i) {
		printf("%d ", array[i]);
	}
	printf("\n");
	fflush(stdout);
}

#undef FUNCNAME
#define FUNCNAME MPIDI_XPMEM_mpi_reduce
#undef FCNAME
#define FCNAME MPL_QUOTE(FUNCNAME)
static inline int MPIDI_XPMEM_mpi_reduce(const void *sendbuf, void *recvbuf, int count,
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
	// if (myrank == root) {
	// 	printf("Rank: %d, enter MPIDI_XPMEM_mpi_reduce with sendbuf[9] %d\n", myrank, ((const int*)sendbuf)[9]);
	// 	fflush(stdout);
	// }
	// while (1);
	// printf("Rank: %d, enter MPIDI_XPMEM_mpi_reduce\n", myrank);
	// fflush(stdout);
	int psize = comm->local_size;
	int errLine;
	void *destdataBuf = NULL;
	void *destrealBuf = NULL;
	ackHeader destheader;
	xpmem_apid_t destapid;

	void *srcdataBuf = NULL;
	void *srcrealBuf = NULL;
	ackHeader srcHeader;
	xpmem_apid_t srcapid;

	size_t dataSz, typesize;
	if (count == 0)
		goto fn_exit;
	// if (myrank == root) {
	// 	// printf("rank %d sendbuf: ", myrank);
	// 	if (sendbuf != MPI_IN_PLACE) {
	// 		printArray(myrank, sendbuf, count);
	// 	} else {
	// 		printArray(myrank, recvbuf, count);
	// 	}
	// 	// printf("\n");
	// 	// fflush(stdout);
	// } else {
	// 	printArray(myrank, sendbuf, count);
	// }
	// COLL_SHMEM_MODULE = POSIX_MODULE;
	// MPIDI_POSIX_mpi_barrier(comm, errflag, NULL);
	// COLL_SHMEM_MODULE = XPMEM_MODULE;

	// if (myrank != root) {
	// 	printArray(myrank, sendbuf, count);
	// }
	typesize =  MPIR_Datatype_get_basic_size(datatype);
	dataSz = typesize * count;
	/* Attach destination buffer located on root process */
	if (myrank == root) {
		// 	*(__s64*)(destheader + 1) = 1L;
		// 	*(__s64*)(destheader + 1) = 0L;
		destdataBuf = recvbuf;
		// printf("Rank: %d, enter MPIDI_XPMEM_mpi_reduce with recvbuf[9] %d\n", myrank, ((int*)recvbuf)[9]);
		// fflush(stdout);
		// printf("dataSz=%d, typesize=%d\n", dataSz, typesize);
		mpi_errno = xpmemExposeMem(destdataBuf, dataSz, &destheader);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}

		if (sendbuf != MPI_IN_PLACE) {
			memcpy(recvbuf, sendbuf, dataSz);
		}
		// printf("Rank: %d, root %d expose dest buffer with handler %llX\n", myrank, root, destheader.dtHandler);
		// fflush(stdout);
	}
	COLL_SHMEM_MODULE = POSIX_MODULE;
	mpi_errno = MPIDI_POSIX_mpi_bcast(&destheader, 4, MPI_LONG_LONG, root, comm, errflag, NULL);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	COLL_SHMEM_MODULE = XPMEM_MODULE;
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