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

#ifndef PIP_SYNC
	mpi_errno = MPIDI_POSIX_mpi_recv(&rmaddr, 2, MPI_LONG_LONG, rank, tag, comm, context_offset, status, request);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	MPIR_Request *sendReq;
	mpi_errno = MPIDI_POSIX_mpi_send(&myaddr, 1, MPI_LONG_LONG, rank, 0, comm, context_offset, NULL, &sendReq);
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
#endif


#ifdef PIP_PROFILE_MISS
	long long sumv[2] = {0, 0};
#ifdef PIP_COMBINE_MISS
	int EventSet = PAPI_NULL;
	int *events = NULL;
	long long values[4] = {0, 0, 0, 0};
	int retval;
	if ((retval = PAPI_create_eventset(&EventSet)) != PAPI_OK) {
		fprintf(stderr, "PAPI_create_eventset error %d\n", retval);
		exit(1);
	}
	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_L1" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_L2" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "PAGE_WALKER_LOADS:DTLB_MEMORY" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}

	retval = PAPI_add_named_event( EventSet, "OFFCORE_RESPONSE_0:L3_MISS" );
	if ( retval != PAPI_OK ) {
		printf("Error : %s\n", PAPI_strerror(retval));
		return -1;
	}
#else
	long long values[2];
#ifdef TLB_MISS
	int events[2] = {PAPI_PRF_DM, PAPI_TLB_DM};
#else
	int events[2] = {PAPI_PRF_DM, PAPI_L3_TCM};
#endif
#endif

	// int myrank = ;
	// char buffer[8];
	// char file[64] = "pip-recv_";
	// double synctime = 0.0, copytime = 0.0;
	FILE *fp;
	mpi_errno = papiStart(events, "PIP-recv_", comm->rank, rmaddr.dataSz, &fp, &EventSet);

#endif


#ifndef PIP_MEMCOPY
	long long ssize = rmaddr.dataSz / 2L;
	void *src = (void*) rmaddr.addr;
#ifdef PIP_SYNC
	static char buffer[1024];
	ssize = 512;
	src = buffer;
#endif
	memcpy(buf, src, ssize);
#endif

#ifdef PIP_PROFILE_MISS
#ifdef PIP_COMBINE_MISS
	if ((retval = PAPI_stop(EventSet, values)) != PAPI_OK) {
		printf("Error : %s\n", PAPI_strerror(retval));
		errLine = __LINE__;
		goto fn_fail;
	}
	sumv[0] = values[0] + values[1] + values[2];
	sumv[1] = values[3];
	PAPI_cleanup_eventset(EventSet);
	PAPI_destroy_eventset(&EventSet);
#else
	if (PAPI_stop_counters(values, 2) != PAPI_OK) {
		mpi_errno = MPI_ERR_OTHER;
		errLine = __LINE__;
		goto fn_fail;
	}
	sumv[0] = values[0];
	sumv[1] = values[1];
#endif
	fprintf(fp, "%lld %lld\n", sumv[0], sumv[1]);
	fclose(fp);
#endif


	if (status != MPI_STATUS_IGNORE) {
		MPIR_STATUS_SET_COUNT(*status, rmaddr.dataSz);
		status->MPI_SOURCE = rank;
		status->MPI_TAG = tag;
	}

	int ack;
// #ifdef STAGE_PROFILE
// 	synctime -= MPI_Wtime();
// #endif

#ifndef PIP_SYNC
	mpi_errno = MPIDI_POSIX_mpi_send(&ack, 1, MPI_INT, rank, 0, comm, context_offset, NULL, request);
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
#endif

	goto fn_exit;

fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	*request = NULL;
	return mpi_errno;
}

#endif