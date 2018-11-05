#ifndef PIP_SEND_INCLUDED
#define PIP_SEND_INCLUDED

#include "pip_progress.h"
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"
// #include "../posix/posix_impl.h"
/* ---------------------------------------------------- */
/* MPIDI_XPMEM_do_send                                  */
/* ---------------------------------------------------- */

#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_send)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_mpi_send(const void *buf, MPI_Aint count,
        MPI_Datatype datatype, int rank, int tag,
        MPIR_Comm * comm, int context_offset,
        MPIDI_av_entry_t * addr, MPIR_Request ** request) {
	int mpi_errno = MPI_SUCCESS;
	size_t dataSz;
	int errLine;

	dataSz = MPIR_Datatype_get_basic_size(datatype) * count;
	pipHeader myaddr;
	long long rmaddr;

	myaddr.addr = (long long) buf;
	myaddr.dataSz = (long long) dataSz;

	mpi_errno = MPIDI_POSIX_mpi_send(&myaddr, 2, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_recv(&rmaddr, 1, MPI_LONG_LONG, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}


	if (*request != NULL) {
		mpi_errno = MPID_PIP_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}

	// printf("Sender myaddr= %llX, receiver rmaddr= %llX\n", myaddr.addr, rmaddr);
	// fflush(stdout);

	long long sindex = dataSz / 2L;
	long long ssize = dataSz - sindex;
	char *dest = (char*) rmaddr + sindex;
	char *src = (char*) myaddr.addr + sindex;
	memcpy(dest, src, ssize);


	/* Wait */
	int ack;
	// MPI_Status ackStatus;
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	if (*request != NULL) {
		mpi_errno = MPID_PIP_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}



	// if (dataSz <= COOP_COPY_DATA_THRESHOLD) {
	// 	/* Send buf memory addr to receiver */
	// 	/* Sender just waits for receiver's ack */
	// 	pipHeader myaddr = (long long) buf;

	// 	mpi_errno = MPIDI_POSIX_mpi_send(&addr, 1, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}

	// 	/* Wait */
	// 	int ack;
	// 	// MPI_Status ackStatus;
	// 	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, tag, comm, context_offset, MPI_STATUS_IGNORE, request);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}

	// 	if (*request != NULL) {
	// 		mpi_errno = MPID_PIP_Wait(*request);
	// 		if (mpi_errno != MPI_SUCCESS) {
	// 			errLine = __LINE__;
	// 			goto fn_fail;
	// 		}
	// 	}
	// } else {
	// 	printf("To be implemented\n");
	// 	fflush(stdout);
	// }


	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	fflush(stdout);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif