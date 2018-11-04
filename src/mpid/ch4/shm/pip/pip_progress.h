#ifndef PIP_PROGRESS_INCLUDED
#define PIP_PROGRESS_INCLUDED

int MPIR_Wait_impl(MPIR_Request * request_ptr, MPI_Status * status);

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