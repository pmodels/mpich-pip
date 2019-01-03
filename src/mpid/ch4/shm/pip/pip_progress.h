#ifndef PIP_PROGRESS_INCLUDED
#define PIP_PROGRESS_INCLUDED
#ifdef PIP_PROFILE_MISS
#include <papi.h>
#endif

#define COOP_COPY_DATA_THRESHOLD 4096

#ifdef PROFILE_MISS
extern long long values[2];
#endif

extern char *COLL_SHMEM_MODULE;
int MPIR_Wait_impl(MPIR_Request * request_ptr, MPI_Status * status);

typedef struct pipHeader {
	long long addr;
	long long dataSz;
} pipHeader;

#undef FCNAME
#define FCNAME MPL_QUOTE(MPID_PIP_Wait)
MPL_STATIC_INLINE_PREFIX int MPID_PIP_Wait(MPIR_Request *request_ptr) {
	int mpi_errno = MPI_SUCCESS;
	int errLine;
	if (request_ptr == NULL) {
		goto fn_exit;
	}

	mpi_errno = MPIR_Wait_impl(request_ptr, MPI_STATUS_IGNORE);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}

	mpi_errno = request_ptr->status.MPI_ERROR;
	MPIR_Request_free(request_ptr);
	if (mpi_errno != MPI_SUCCESS) {
		errLine = __LINE__;
		goto fn_fail;
	}
	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}


#ifdef PIP_PROFILE_MISS
#undef FCNAME
#define FCNAME MPL_QUOTE(papiStart)
MPL_STATIC_INLINE_PREFIX int papiStart(int *events, char *prefix, int myrank, int dataSz, FILE **fp, int *eventset) {
	char buffer[8];
	char file[64];
	int errLine, mpi_errno = MPI_SUCCESS;

	strcpy(file, prefix);
	sprintf(buffer, "%d_", myrank);
	strcat(file, buffer);
	sprintf(buffer, "%d", dataSz);
	strcat(file, buffer);
	strcat(file, ".log");
	*fp = fopen(file, "a");
	if (events != NULL) {
		if (PAPI_start_counters(events, 2) != PAPI_OK) {
			mpi_errno = MPI_ERR_OTHER;
			errLine = __LINE__;
			goto fn_fail;
		}
	} else {
		if (PAPI_start(*eventset) != PAPI_OK) {
			printf("Error PAPI_start\n");
			return -1;
		}
	}

	goto fn_exit;
fn_fail:
	printf("[%s-%d] Error with mpi_errno (%d)\n", __FUNCTION__, errLine, mpi_errno);
fn_exit:
	return mpi_errno;
}
#endif
#endif