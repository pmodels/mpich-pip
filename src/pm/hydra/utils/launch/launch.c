/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *  (C) 2008 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#include "hydra.h"
#include "topo.h"

#ifdef HAVE_PIP

#include <pip.h>
#include <pip_ulp.h>

static HYD_status HYDU_pip_envv(struct HYD_env *env_list, char ***envv_ptr)
{
    struct HYD_env *env;
    char **envv = NULL;
    int i, envc;
    HYD_status status = HYD_SUCCESS;

    for (envc = 1, env = env_list; env != NULL; env = env->next)
        envc++;

    HYDU_MALLOC_OR_JUMP(envv, char **, sizeof(char *) * envc, status);

    if (envv != NULL) {
        for (i = 0, env = env_list; env; env = env->next) {
            char *envstr;
            size_t envstr_sz = 0;

            /* FIXME: any better way to get the length of formatted string ? */
            envstr_sz = strlen(env->env_name) + strlen(env->env_value) + 4;

            HYDU_MALLOC_OR_JUMP(envstr, char *, envstr_sz, status);
            sprintf(envstr, "%s=%s", env->env_name, env->env_value);
            envv[i++] = envstr;
        }
        envv[i] = NULL;
    }
    *envv_ptr = envv;

  fn_exit:
    HYDU_FUNC_EXIT();
    return status;

  fn_fail:
    goto fn_exit;
}

struct pip_task_fds {
    int inpipe[2];
    int outpipe[2];
    int errpipe[2];
    int idx;
    char **envv;
    int pmifd;
    int nulps;
};

static int HYDU_pip_before(struct pip_task_fds **fds)
{
    int i;
    int nulps = fds[0]->nulps;
    close(STDIN_FILENO);
    for (i = 0; i < nulps; i++) {
        if (fds[i]->inpipe[0] >= 0) {
            close(fds[i]->inpipe[1]);
            if (fds[i]->inpipe[0] != STDIN_FILENO) {
                while (1) {
                    if (dup2(fds[i]->inpipe[0], STDIN_FILENO) >= 0)
                        break;
                    if (errno != EBUSY)
                        return errno;
                }
            }
        }
    }
    close(STDOUT_FILENO);
    for (i = 0; i < nulps; i++) {
        if (fds[i]->outpipe[0] >= 0) {
            close(fds[i]->outpipe[0]);
            if (fds[i]->outpipe[1] != STDOUT_FILENO) {
                while (1) {
                    if (dup2(fds[i]->outpipe[1], STDOUT_FILENO) >= 0)
                        break;
                    if (errno != EBUSY)
                        return errno;
                }
            }
        }
    }
    close(STDERR_FILENO);
    for (i = 0; i < nulps; i++) {
        if (fds[i]->errpipe[0] >= 0) {
            close(fds[i]->errpipe[0]);
            if (fds[i]->errpipe[1] != STDERR_FILENO) {
                while (1) {
                    if (dup2(fds[i]->errpipe[1], STDERR_FILENO) >= 0)
                        break;
                    if (errno != EBUSY)
                        return errno;
                }
            }
        }
    }
#ifdef AHA
    for (i = 0; i < nulps; i++) {
        if (fds[i]->idx >= 0) {
            if (HYDT_topo_bind(fds[i]->idx) != HYD_SUCCESS) {
                fprintf(stderr, "bind process failed\n");
            }
        }
    }
#endif
    return 0;
}

static int HYDU_pip_after(struct pip_task_fds **fds)
{

    int i;

    int nulps = fds[0]->nulps;
    for (i = 0; i < nulps; i++) {
        int j;
        for (j = 0; fds[i]->envv[j] != NULL; j++)
            MPL_free(fds[i]->envv[j]);
        MPL_free(fds[i]->envv);
    }
//    MPL_free(fds);

    return 0;
}

HYD_status HYDU_spawn_pip_tasks(char **client_arg, struct HYD_env * env_list,
                                int *in, int *out, int *err, int *pid, int idx,
                                int pmifd, int proc_count)
{

    static struct pip_task_fds **fds;
    intptr_t pip_id;
    int tpid = -1;
    int coreno;
    int pip_err;
    HYD_status status = HYD_SUCCESS;

    /* ULP-FIXME need a better way to decide nulps */
    int nulps = atoi(getenv("NULPS"));

    HYDU_FUNC_ENTER();

    if (idx == 0) {
        HYDU_MALLOC_OR_JUMP(fds, struct pip_task_fds **, proc_count * sizeof(struct pip_task_fds *), status);
        int i;
        for (i = 0; i < proc_count; i++) {
            HYDU_MALLOC_OR_JUMP(fds[i], struct pip_task_fds *, sizeof(struct pip_task_fds), status);
        }
    }
    fds[idx]->inpipe[0] = -1;
    fds[idx]->inpipe[1] = -1;
    fds[idx]->outpipe[0] = -1;
    fds[idx]->outpipe[1] = -1;
    fds[idx]->errpipe[0] = -1;
    fds[idx]->errpipe[1] = -1;
    fds[idx]->idx = idx;
    fds[idx]->pmifd = pmifd;

    if (in && (pipe(fds[idx]->inpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    if (out && (pipe(fds[idx]->outpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    if (err && (pipe(fds[idx]->errpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    HYDU_pip_envv(env_list, &fds[idx]->envv);


    if (fds[idx]->idx >= 0) {
        coreno = idx / nulps;

    } else {
        coreno = PIP_CPUCORE_ASIS;
    }


    if (idx % nulps == nulps - 1 ||
        idx == proc_count - 1) {
        pip_ulp_t ulps;
        PIP_ULP_INIT(&ulps);

        int startIdx = idx - (idx % nulps);
        int length = (idx % nulps) + 1;

        int i;
        for (i = startIdx + 1; i < startIdx + length; i++) {
            pip_spawn_program_t prog;

            pip_spawn_from_main(&prog,
                                client_arg[0],
                                client_arg,
                                fds[i]->envv);

            pip_ulp_new(&prog,
                        &i,
                        &ulps,
                        NULL);
        }

        pip_spawn_program_t prog;

        pip_spawn_from_main(&prog,
                            client_arg[0],
                            client_arg,
                            fds[startIdx]->envv);

        fds[startIdx]->nulps = length;

        pip_spawn_hook_t hook;
        pip_spawn_hook(&hook,
                       (pip_spawnhook_t) HYDU_pip_before,
                       (pip_spawnhook_t) HYDU_pip_after,
                       (void *) &fds[startIdx]);

        if ((pip_task_spawn(&prog,
                            coreno,
                            &startIdx,
                            &hook,
                            &ulps)) != 0) {
            HYDU_ERR_SETANDJUMP(status, HYD_FAILURE, "pip_task_spawn(): %s\n", MPL_strerror(pip_err));
        }

        pip_get_id(startIdx, &pip_id);
        tpid = (pid_t) pip_id;

        for (i = startIdx; i < startIdx + length; i++) {
            close(fds[i]->inpipe[0]);
            close(fds[i]->outpipe[1]);
            close(fds[i]->errpipe[1]);
            status = HYDU_sock_cloexec(fds[i]->inpipe[1]);
            HYDU_ERR_POP(status, "unable to set close on exec\n");

            close(fds[i]->pmifd);
        }
        status = HYDU_sock_set_nonblock(fds[startIdx]->inpipe[1]);
        HYDU_ERR_POP(status, "unable to set stdin socket to non-blocking\n");
    }
    if (in) {
        *in = fds[idx]->inpipe[1];
    }
    if (out)
        *out = fds[idx]->outpipe[0];
    if (err)
        *err = fds[idx]->errpipe[0];
    if (pid)
        *pid = tpid;

fn_exit:
    HYDU_FUNC_EXIT();
    return status;

fn_fail:
    goto fn_exit;
}
#endif

HYD_status HYDU_create_process(char **client_arg, struct HYD_env *env_list,
                               int *in, int *out, int *err, int *pid, int idx)
{
    int inpipe[2], outpipe[2], errpipe[2], tpid;
    HYD_status status = HYD_SUCCESS;

    HYDU_FUNC_ENTER();

    if (in && (pipe(inpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    if (out && (pipe(outpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    if (err && (pipe(errpipe) < 0))
        HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "pipe error (%s)\n", MPL_strerror(errno));

    /* Fork off the process */
    tpid = fork();
    if (tpid == 0) {    /* Child process */

#if defined HAVE_SETSID
        setsid();
#endif /* HAVE_SETSID */

        close(STDIN_FILENO);
        if (in) {
            close(inpipe[1]);
            if (inpipe[0] != STDIN_FILENO && dup2(inpipe[0], STDIN_FILENO) < 0)
                HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "dup2 error (%s)\n",
                                    MPL_strerror(errno));
        }

        close(STDOUT_FILENO);
        if (out) {
            close(outpipe[0]);
            if (outpipe[1] != STDOUT_FILENO && dup2(outpipe[1], STDOUT_FILENO) < 0)
                HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "dup2 error (%s)\n",
                                    MPL_strerror(errno));
        }

        close(STDERR_FILENO);
        if (err) {
            close(errpipe[0]);
            if (errpipe[1] != STDERR_FILENO && dup2(errpipe[1], STDERR_FILENO) < 0)
                HYDU_ERR_SETANDJUMP(status, HYD_SOCK_ERROR, "dup2 error (%s)\n",
                                    MPL_strerror(errno));
        }

        /* Forced environment overwrites existing environment */
        if (env_list) {
            status = HYDU_putenv_list(env_list, HYD_ENV_OVERWRITE_TRUE);
            HYDU_ERR_POP(status, "unable to putenv\n");
        }

        if (idx >= 0) {
            status = HYDT_topo_bind(idx);
            HYDU_ERR_POP(status, "bind process failed\n");
        }

        if (execvp(client_arg[0], client_arg) < 0) {
            /* The child process should never get back to the proxy
             * code; if there is an error, just throw it here and
             * exit. */
            HYDU_error_printf("execvp error on file %s (%s)\n", client_arg[0], MPL_strerror(errno));
            exit(-1);
        }
    }
    else {      /* Parent process */
        if (out)
            close(outpipe[1]);
        if (err)
            close(errpipe[1]);
        if (in) {
            close(inpipe[0]);
            *in = inpipe[1];

            status = HYDU_sock_cloexec(*in);
            HYDU_ERR_POP(status, "unable to set close on exec\n");
        }
        if (out)
            *out = outpipe[0];
        if (err)
            *err = errpipe[0];
    }

    if (pid)
        *pid = tpid;

  fn_exit:
    HYDU_FUNC_EXIT();
    return status;

  fn_fail:
    goto fn_exit;
}
