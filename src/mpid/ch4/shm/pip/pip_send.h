#ifndef PIP_SEND_INCLUDED
#define PIP_SEND_INCLUDED

#include "pip_progress.h"
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"

/* ---------------------------------------------------- */
/* MPIDI_XPMEM_do_send                                  */
/* ---------------------------------------------------- */

#undef FCNAME
#define FCNAME DECL_FUNC(MPIDI_XPMEM_SEND)
MPL_STATIC_INLINE_PREFIX int MPIDI_PIP_mpi_send(const void *buf, MPI_Aint count,
        MPI_Datatype datatype, int rank, int tag,
        MPIR_Comm * comm, int context_offset,
        MPIDI_av_entry_t * addr, MPIR_Request ** request) {
	int mpi_errno = MPI_SUCCESS;
	size_t dataSz;
	int errLine;

	dataSz = MPIR_Datatype_get_basic_size(datatype) * count;

	/* Send buf memory addr to receiver */
	ackHeader header;
	mpi_errno = MPIDI_POSIX_mpi_send(&header.dataSz, 4, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}


	/* Wait */
	int ack;
	MPI_Status ackStatus;
	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, tag, comm, context_offset, &ackStatus, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	if (*request != NULL) {
		mpi_errno = MPID_XPMEM_Wait(*request);
		if (mpi_errno != MPI_SUCCESS) {
			errLine = __LINE__;
			goto fn_fail;
		}
	}

	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
	fflush(stdout);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif