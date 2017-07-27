/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2016 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "mpi.h"
#include <stdio.h>
#include <unistd.h>


int main(int argc, char *argv[])
{
    int myid, numprocs;
    long rand = 0;
    unsigned long rand_addr = 0;
    int namelen;
    char processor_name[MPI_MAX_PROCESSOR_NAME];
    char host_name[256];
    int err = 0, errs = 0;
    MPI_Comm shm_comm = MPI_COMM_NULL;
    int shm_numprocs, shm_myid;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &myid);
    MPI_Get_processor_name(processor_name, &namelen);

    err = gethostname(host_name, 256);
    if(err) {
        fprintf(stderr, "Process %d cannot get hostname\n", myid);
        fflush(stderr);
    }

    MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &shm_comm);
    MPI_Comm_size(shm_comm, &shm_numprocs);
    MPI_Comm_rank(shm_comm, &shm_myid);

    rand_addr = (unsigned long) &rand;
    MPI_Bcast(&rand_addr, 1, MPI_UNSIGNED_LONG, 0, shm_comm);

    fprintf(stdout, "Process %d of %d (%d of %d in shm) is on %s, host %s, "
            "checking rand at 0x%lx\n", myid, numprocs, shm_myid, shm_numprocs,
            processor_name, (err == 0) ? host_name : "", rand_addr);
    fflush(stdout);

    /* Only the first local process sets and bcast.  */
    if (shm_myid == 0)
        rand = (long) (MPI_Wtime() * 1200) * (myid + 1);
    MPI_Bcast(&rand, 1, MPI_LONG, 0, shm_comm);

    /* Directly access remote address and compare value */
    if (*((long *) rand_addr) != rand) {
        fprintf(stderr, "Process %d: remote wtime_addr 0x%lx %ld != rand %ld\n",
                myid, rand_addr, *((long *) rand_addr), rand);
        fflush(stderr);
        err++;
    }

    MPI_Reduce(&err, &errs, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    if (myid == 0 && errs == 0) {
        fprintf(stdout, "No errors\n");
        fflush(stdout);
    }

    MPI_Comm_free(&shm_comm);
    MPI_Finalize();
    return 0;
}
