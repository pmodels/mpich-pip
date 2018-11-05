#ifndef XPMEM_RECV_H_INCLUDED
#define XPMEM_RECV_H_INCLUDED

#include "pip_progress.h"
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"
// #include "../posix/posix_impl.h"

/* ---------------------------------------------------- */
/* MPIDI_PIP_mpi_recv                                             */
/* ---------------------------------------------------- */


#undef FCNAME
#define FCNAME MPL_QUOTE(MPIDI_PIP_mpi_recv)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_mpi_recv(void *buf,
        MPI_Aint count,
        MPI_Datatype datatype,
        int rank,
        int tag,
        MPIR_Comm * comm,
        int context_offset, MPI_Status * status,
        MPIR_Request ** request) {

	int mpi_errno = MPI_SUCCESS;
	int errLine;

	long long myaddr = (long long) buf;
	pipHeader rmaddr;

	mpi_errno = MPIDI_POSIX_mpi_recv(&rmaddr, 2, MPI_LONG_LONG, rank, tag, comm, context_offset, status, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	MPIR_Request *sendReq;
	mpi_errno = MPIDI_POSIX_mpi_send(&myaddr, 1, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, &sendReq);
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

	printf("Receiver myaddr= %llX, sender rmaddr= %llX\n", myaddr, rmaddr.addr);
	fflush(stdout);

	long long ssize = rmaddr.dataSz / 2L;
	void *src = (void*) rmaddr.addr;
	memcpy(buf, src, ssize);

	if (status != MPI_STATUS_IGNORE) {
		MPIR_STATUS_SET_COUNT(*status, rmaddr.dataSz);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
	}

	int ack;
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
	// 	/* Get data handler in order to attach memory page from source process */
	// 	// ackHeader header;
	// 	mpi_errno = MPIDI_POSIX_mpi_recv(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, status, request);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}

	// 	if (*request != NULL) {
	// 		// printf("Recv wait\n");
	// 		// fflush(stdout);
	// 		mpi_errno = MPID_XPMEM_Wait(*request);
	// 		if (mpi_errno != MPI_SUCCESS) {
	// 			errLine = __LINE__;
	// 			goto fn_fail;
	// 		}
	// 	}
	// 	// printf("Recv header.dtHandler %llX\n", header.dtHandler);
	// 	// fflush(stdout);
	// 	void *dataBuffer, *realBuffer;
	// 	xpmem_apid_t apid;
	// 	// double time = MPI_Wtime();
	// 	mpi_errno = xpmemAttachMem(&header, &dataBuffer, &realBuffer, &apid);
	// 	// time = MPI_Wtime() - time;
	// 	// printf("xpmemAttachMem time= %.6lf\n", time);
	// 	// fflush(stdout);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}

	// 	/* Copy data by dataSz bytes */
	// 	// time = MPI_Wtime();
	// 	MPIR_Memcpy(buf, dataBuffer, header.dataSz);
	// 	// time = MPI_Wtime() - time;
	// 	// printf("copy time= %.6lf\n", time);
	// 	// fflush(stdout);
	// 	// printf("Receiver enter infinite loop\n");
	// 	// fflush(stdout);
	// 	// sleep(10);
	// 	if (status != MPI_STATUS_IGNORE) {
	// 		MPIR_STATUS_SET_COUNT(*status, header.dataSz);
	// 		status->MPI_SOURCE = rank;
	// 		status->MPI_TAG = tag;
	// 	}

	// 	int ack;
	// 	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, tag, comm, context_offset, NULL, request);
	// 	if (mpi_errno != MPI_SUCCESS) {
	// 		errLine = __LINE__;
	// 		goto fn_fail;
	// 	}
	// } else {

	// }

	goto fn_exit;

fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif