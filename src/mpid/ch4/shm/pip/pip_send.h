#ifndef PIP_SEND_INCLUDED
#define PIP_SEND_INCLUDED

#include "pip_progress.h"
#include "../posix/posix_send.h"
#include "../posix/posix_recv.h"
// #include <papi.h>
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
#ifdef STAGE_PROFILE
	int events[2] = {PAPI_L3_TCM, PAPI_TLB_DM};
	long long values[2];
	int myrank = comm->rank;
	char buffer[8];
	char file[64] = "pip-send_";
	double synctime = 0.0, copytime = 0.0;

	sprintf(buffer, "%d_", myrank);
	strcat(file, buffer);
	sprintf(buffer, "%ld", dataSz);
	strcat(file, buffer);
	strcat(file, ".log");
	FILE *fp = fopen(file, "a");
	synctime -= MPI_Wtime();
#endif
	mpi_errno = MPIDI_POSIX_mpi_send(&myaddr, 2, MPI_LONG_LONG, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_recv(&rmaddr, 1, MPI_LONG_LONG, rank, 0, comm, context_offset, MPI_STATUS_IGNORE, request);
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
#ifdef STAGE_PROFILE
	synctime += MPI_Wtime();
#endif

	// printf("Sender myaddr= %llX, receiver rmaddr= %llX\n", myaddr.addr, rmaddr);
	// fflush(stdout);

	long long sindex = dataSz / 2L;
	long long ssize = dataSz - sindex;
	char *dest = (char*) rmaddr + sindex;
	char *src = (char*) myaddr.addr + sindex;

#ifdef STAGE_PROFILE
	if (PAPI_start_counters(events, 2) != PAPI_OK) {
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}
	copytime -= MPI_Wtime();
#endif
	memcpy(dest, src, ssize);
#ifdef STAGE_PROFILE
	copytime += MPI_Wtime();
	if (PAPI_stop_counters(values, 2) != PAPI_OK) {
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}
#endif

	/* Wait */
	int ack;
	// MPI_Status ackStatus;
#ifdef STAGE_PROFILE
	synctime -= MPI_Wtime();
#endif
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, tag, comm, context_offset, NULL, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = MPIDI_POSIX_mpi_recv(&ack, 1, MPI_INT, rank, 0, comm, context_offset, MPI_STATUS_IGNORE, request);
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
#ifdef STAGE_PROFILE
	synctime += MPI_Wtime();

	fprintf(fp, "%.8lf 0.0 %.8lf %lld %lld\n", synctime, copytime, values[0], values[1]);
	fclose(fp);
#endif
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