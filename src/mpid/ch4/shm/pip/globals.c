/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2006 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 *
 *  Portions of this code were written by Intel Corporation.
 *  Copyright (C) 2011-2016 Intel Corporation.  Intel provides this material
 *  to Argonne National Laboratory subject to Software Grant and Corporate
 *  Contributor License Agreement dated February 8, 2012.
 */

#include "pip_pre.h"

MPIDI_PIP_global_t pip_global;

MPIDI_PIP_task_t MPIDI_Task_direct[MPIDI_TASK_PREALLOC] = { 0 };

DLOOP_Segment MPIDI_Segment_direct[MPIDI_SEGMENT_PREALLOC] = { 0 };

MPIR_Object_alloc_t MPIDI_Task_mem = {
    0, 0, 0, 0, MPIDI_TASK, sizeof(MPIDI_PIP_task_t), MPIDI_Task_direct,
    MPIDI_TASK_PREALLOC
};

MPIR_Object_alloc_t MPIDI_Segment_mem = {
    0, 0, 0, 0, MPIDI_SEGMENT, sizeof(DLOOP_Segment), MPIDI_Segment_direct,
    MPIDI_SEGMENT_PREALLOC
};
