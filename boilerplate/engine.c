/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

/* ------------------------------------------------------------------ */
/* Constants                                                           */
/* ------------------------------------------------------------------ */

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 512
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

/* ------------------------------------------------------------------ */
/* Enums                                                               */
/* ------------------------------------------------------------------ */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

/* ------------------------------------------------------------------ */
/* Data structures                                                     */
/* ------------------------------------------------------------------ */

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int stop_requested;   /* set before we send SIGTERM so reaper can classify */
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* producer thread argument */
typedef struct {
    int fd;
    char id[CONTAINER_ID_LEN];
    supervisor_ctx_t *ctx;
} prod_arg_t;

/* ------------------------------------------------------------------ */
/* Signal-handler globals                                              */
/* ------------------------------------------------------------------ */

static volatile sig_atomic_t g_got_sigchld = 0;
static volatile sig_atomic_t g_shutdown    = 0;
static supervisor_ctx_t     *g_ctx         = NULL;

/* ------------------------------------------------------------------ */
/* Usage / parsing helpers (unchanged from boilerplate)               */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ------------------------------------------------------------------ */
/* Bounded buffer                                                      */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * bounded_buffer_push — producer side
 *
 * Blocks while full (unless shutdown starts, in which case we drop
 * the item and return -1 so the producer thread exits cleanly).
 * Signals one waiting consumer on success.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->head] = *item;
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop — consumer side
 *
 * Blocks while empty. Returns -1 only when the buffer is both empty
 * AND shutting down — this is the signal for the consumer thread to exit.
 * If items remain after shutdown starts, we keep draining them (no data loss).
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0) {
        /* shutting_down must be set, nothing left to drain */
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->tail];
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Logging consumer thread (Step 4)                                   */
/* ------------------------------------------------------------------ */

/*
 * logging_thread
 *
 * Pops log chunks from the bounded buffer and appends them to the
 * correct per-container log file. Keeps a small open-fd cache so we
 * don't re-open the file on every chunk. Exits when bounded_buffer_pop
 * returns -1 (buffer empty + shutdown).
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;

    /* Simple open-fd cache: parallel arrays, linear search is fine for <= 16 containers */
#define LOG_CACHE_SIZE 16
    char   cached_id[LOG_CACHE_SIZE][CONTAINER_ID_LEN];
    FILE  *cached_fp[LOG_CACHE_SIZE];
    int    cache_count = 0;
    int    i;

    memset(cached_id, 0, sizeof(cached_id));
    memset(cached_fp, 0, sizeof(cached_fp));

    log_item_t item;

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        /* Find or open the log file for this container */
        FILE *fp = NULL;
        for (i = 0; i < cache_count; i++) {
            if (strcmp(cached_id[i], item.container_id) == 0) {
                fp = cached_fp[i];
                break;
            }
        }

        if (!fp) {
            /* Look up log path from metadata */
            char log_path[PATH_MAX] = {0};
            pthread_mutex_lock(&ctx->metadata_lock);
            container_record_t *rec = ctx->containers;
            while (rec) {
                if (strcmp(rec->id, item.container_id) == 0) {
                    strncpy(log_path, rec->log_path, sizeof(log_path) - 1);
                    break;
                }
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            if (log_path[0] != '\0') {
                fp = fopen(log_path, "a");
                if (fp && cache_count < LOG_CACHE_SIZE) {
                    strncpy(cached_id[cache_count], item.container_id,
                            CONTAINER_ID_LEN - 1);
                    cached_fp[cache_count] = fp;
                    cache_count++;
                }
            }
        }

        if (fp) {
            fwrite(item.data, 1, item.length, fp);
            fflush(fp);
        }
    }

    /* Close all cached file handles */
    for (i = 0; i < cache_count; i++) {
        if (cached_fp[i])
            fclose(cached_fp[i]);
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/* Producer thread — one per container                                */
/* ------------------------------------------------------------------ */

/*
 * producer_thread
 *
 * Reads raw bytes from the container's pipe read-end and pushes
 * log_item_t chunks into the bounded buffer. Exits when read() returns
 * 0 (container closed its end of the pipe — i.e. it exited).
 */
void *producer_thread(void *arg)
{
    prod_arg_t *parg = (prod_arg_t *)arg;
    char buf[LOG_CHUNK_SIZE];
    ssize_t n;

    while ((n = read(parg->fd, buf, sizeof(buf))) > 0) {
        log_item_t item;
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, parg->id, sizeof(item.container_id) - 1);
        item.length = (size_t)n;
        memcpy(item.data, buf, (size_t)n);
        /* push returns -1 on shutdown — stop reading */
        if (bounded_buffer_push(&parg->ctx->log_buffer, &item) != 0)
            break;
    }

    close(parg->fd);
    free(parg);
    return NULL;
}

/* ------------------------------------------------------------------ */
/* Container child entrypoint (Step 2)                                */
/* ------------------------------------------------------------------ */

/*
 * child_fn — runs inside the new namespaces after clone().
 *
 * 1. Redirect stdout/stderr to the supervisor pipe
 * 2. Apply nice value
 * 3. Set hostname (isolated UTS namespace)
 * 4. chroot into the container's own rootfs copy
 * 5. Mount /proc inside the new mount namespace
 * 6. execve the requested command
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Redirect stdout and stderr into the logging pipe */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    /* Apply scheduling priority */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) < 0)
            perror("nice");  /* non-fatal */
    }

    /* Set container hostname using its ID (UTS ns is isolated) */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    /* Jail into the container's writable rootfs copy */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir");
        return 1;
    }

    /* Mount /proc so ps, top, etc. work inside the container.
     * This is safe here because we have our own mount namespace (CLONE_NEWNS). */
    if (mount("proc", "/proc", "proc", 0, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    /* exec the requested command */
    char *argv_exec[] = { cfg->command, NULL };
    char *envp[]      = {
        "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "HOME=/root",
        "TERM=xterm",
        NULL
    };

    execve(cfg->command, argv_exec, envp);
    perror("execve");  /* only reached on failure */
    return 1;
}

/* ------------------------------------------------------------------ */
/* Kernel monitor helpers (unchanged from boilerplate)                */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/* Signal handlers                                                     */
/* ------------------------------------------------------------------ */

static void sigchld_handler(int sig)
{
    (void)sig;
    g_got_sigchld = 1;
}

static void shutdown_handler(int sig)
{
    (void)sig;
    g_shutdown = 1;
}

/* ------------------------------------------------------------------ */
/* Child reaper — called from the event loop on SIGCHLD               */
/* ------------------------------------------------------------------ */

/*
 * reap_children
 *
 * Calls waitpid in a loop (WNOHANG) to collect all exited children.
 * Updates container metadata:
 *   - normal exit        → CONTAINER_EXITED,  exit_code = actual code
 *   - stop_requested set → CONTAINER_STOPPED, exit_code = 128 + signal
 *   - SIGKILL, no stop   → CONTAINER_KILLED   (hard limit kill from kernel)
 */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        pthread_mutex_lock(&ctx->metadata_lock);

        container_record_t *rec = ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code   = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    rec->state       = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code   = 128 + rec->exit_signal;
                    /*
                     * Attribution rule (from project guide):
                     *   stop_requested set → we asked for it → STOPPED
                     *   SIGKILL, no stop   → kernel monitor killed it → KILLED
                     */
                    if (rec->stop_requested)
                        rec->state = CONTAINER_STOPPED;
                    else
                        rec->state = CONTAINER_KILLED;
                }
                break;
            }
            rec = rec->next;
        }

        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ------------------------------------------------------------------ */
/* spawn_container — used by CMD_START and CMD_RUN                    */
/* ------------------------------------------------------------------ */

static container_record_t *spawn_container(supervisor_ctx_t *ctx,
                                           const control_request_t *req,
                                           char *errbuf,
                                           size_t errlen)
{
    int pipefd[2];
    pid_t pid;
    char *stack;
    child_config_t *cfg;
    container_record_t *rec;

    /* Reject duplicate live container IDs */
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *existing = ctx->containers;
    while (existing) {
        if (strcmp(existing->id, req->container_id) == 0 &&
            (existing->state == CONTAINER_RUNNING ||
             existing->state == CONTAINER_STARTING)) {
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(errbuf, errlen, "container '%s' already running",
                     req->container_id);
            return NULL;
        }
        existing = existing->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    /* Pipe: container writes, supervisor reads */
    if (pipe(pipefd) < 0) {
        snprintf(errbuf, errlen, "pipe: %s", strerror(errno));
        return NULL;
    }

    /* Stack for clone() — grows downward, so pass top */
    stack = malloc(STACK_SIZE);
    if (!stack) {
        snprintf(errbuf, errlen, "malloc stack: %s", strerror(errno));
        close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }

    /* Child configuration — child_fn reads this before exec */
    cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        snprintf(errbuf, errlen, "malloc cfg: %s", strerror(errno));
        free(stack); close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      req->container_id, sizeof(cfg->id)      - 1);
    strncpy(cfg->rootfs,  req->rootfs,       sizeof(cfg->rootfs)  - 1);
    strncpy(cfg->command, req->command,      sizeof(cfg->command) - 1);
    cfg->nice_value   = req->nice_value;
    cfg->log_write_fd = pipefd[1];

    /* Ensure log directory exists */
    mkdir(LOG_DIR, 0755);

    /* Allocate and populate metadata record */
    rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        snprintf(errbuf, errlen, "calloc record: %s", strerror(errno));
        free(cfg); free(stack); close(pipefd[0]); close(pipefd[1]);
        return NULL;
    }
    strncpy(rec->id, req->container_id, sizeof(rec->id) - 1);
    rec->state            = CONTAINER_STARTING;
    rec->started_at       = time(NULL);
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    snprintf(rec->log_path, sizeof(rec->log_path),
             "%s/%s.log", LOG_DIR, req->container_id);

    /*
     * clone() into isolated PID, UTS, and mount namespaces.
     * SIGCHLD ensures the supervisor gets a signal when the child exits.
     */
    pid = clone(child_fn,
                stack + STACK_SIZE,
                CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                cfg);

    /* Parent closes write end — child owns it. Free the stack (already copied). */
    close(pipefd[1]);
    free(stack);

    if (pid < 0) {
        snprintf(errbuf, errlen, "clone: %s", strerror(errno));
        free(cfg); free(rec); close(pipefd[0]);
        return NULL;
    }

    rec->host_pid = pid;
    rec->state    = CONTAINER_RUNNING;

    /* Register with kernel memory monitor (non-fatal if module absent) */
    if (ctx->monitor_fd >= 0) {
        register_with_monitor(ctx->monitor_fd,
                              req->container_id,
                              pid,
                              req->soft_limit_bytes,
                              req->hard_limit_bytes);
    }

    /* Prepend to metadata list */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec->next       = ctx->containers;
    ctx->containers = rec;
    pthread_mutex_unlock(&ctx->metadata_lock);

    /*
     * Start a detached producer thread to drain this container's pipe.
     * It exits automatically when the container closes the write end.
     */
    prod_arg_t *parg = malloc(sizeof(prod_arg_t));
    if (!parg) {
        /* Non-fatal: we just won't capture logs for this container */
        close(pipefd[0]);
    } else {
        parg->fd  = pipefd[0];
        parg->ctx = ctx;
        strncpy(parg->id, req->container_id, sizeof(parg->id) - 1);

        pthread_t prod_tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&prod_tid, &attr, producer_thread, parg);
        pthread_attr_destroy(&attr);
    }

    free(cfg);
    return rec;
}

/* ------------------------------------------------------------------ */
/* Supervisor — main event loop (Step 3)                              */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx          = &ctx;

    /* Init shared state */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init"); return 1; }

    /* 1. Open kernel monitor device (non-fatal — monitor may not be loaded yet) */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "Warning: /dev/container_monitor unavailable: %s\n",
                strerror(errno));

    /* 2. Create and bind the UNIX domain control socket */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    unlink(CONTROL_PATH);  /* remove stale socket from a previous run */
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen"); return 1;
    }

    /* 3. Install signal handlers */
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = shutdown_handler;
    sa.sa_flags   = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* 4. Start the logging consumer thread */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create logger"); return 1; }

    fprintf(stderr, "Supervisor ready. base-rootfs: %s  socket: %s\n",
            rootfs, CONTROL_PATH);

    /* 5. Event loop — select() with 1-second timeout so we check g_shutdown */
    while (!g_shutdown) {
        fd_set rfds;
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);

        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) {
                if (g_got_sigchld) { g_got_sigchld = 0; reap_children(&ctx); }
                continue;
            }
            perror("select");
            break;
        }

        /* Reap any children that exited during the select wait */
        if (g_got_sigchld) { g_got_sigchld = 0; reap_children(&ctx); }

        if (!FD_ISSET(ctx.server_fd, &rfds))
            continue;

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }

        control_request_t  req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        if (read(client_fd, &req, sizeof(req)) != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        /* ---- Dispatch ---- */
        switch (req.kind) {

        /* -- start: launch container in background -- */
        case CMD_START: {
            char errbuf[256] = {0};
            container_record_t *rec = spawn_container(&ctx, &req, errbuf, sizeof(errbuf));
            if (rec) {
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "started '%s'  pid=%d  log=%s/%s.log",
                         rec->id, rec->host_pid, LOG_DIR, rec->id);
            } else {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "start failed: %s", errbuf);
            }
            break;
        }

        /* -- run: launch and block until container exits -- */
        case CMD_RUN: {
            char errbuf[256] = {0};
            container_record_t *rec = spawn_container(&ctx, &req, errbuf, sizeof(errbuf));
            if (!rec) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "run failed: %s", errbuf);
                write(client_fd, &resp, sizeof(resp));
                close(client_fd);
                continue;
            }
            pid_t run_pid = rec->host_pid;
            int wstatus;
            /* Block here until the specific container exits */
            while (waitpid(run_pid, &wstatus, 0) < 0 && errno == EINTR)
                ;
            reap_children(&ctx);  /* update metadata for this and any other exited children */

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *r = ctx.containers;
            while (r && r->host_pid != run_pid) r = r->next;
            if (r) {
                resp.status = r->exit_code;
                snprintf(resp.message, sizeof(resp.message),
                         "'%s' exited: code=%d signal=%d state=%s",
                         r->id, r->exit_code, r->exit_signal,
                         state_to_string(r->state));
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            break;
        }

        /* -- stop: send SIGTERM, mark stop_requested -- */
        case CMD_STOP: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec && strcmp(rec->id, req.container_id) != 0)
                rec = rec->next;

            if (rec && rec->state == CONTAINER_RUNNING) {
                rec->stop_requested = 1;
                rec->state          = CONTAINER_STOPPED;
                kill(rec->host_pid, SIGTERM);
                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "sent SIGTERM to '%s' (pid %d)",
                         req.container_id, rec->host_pid);

                /* Unregister from kernel monitor */
                if (ctx.monitor_fd >= 0)
                    unregister_from_monitor(ctx.monitor_fd,
                                            req.container_id,
                                            rec->host_pid);
            } else {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "'%s' not found or not running", req.container_id);
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            break;
        }

        /* -- ps: list all tracked containers -- */
        case CMD_PS: {
            char buf[CONTROL_MESSAGE_LEN];
            int offset = 0;
            offset += snprintf(buf + offset, sizeof(buf) - offset,
                               "%-16s %-7s %-8s %-12s %-10s %-10s\n",
                               "ID", "PID", "STATE", "STARTED",
                               "SOFT(MiB)", "HARD(MiB)");

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec && offset < (int)sizeof(buf) - 1) {
                char tstr[16];
                struct tm *tm_info = localtime(&rec->started_at);
                strftime(tstr, sizeof(tstr), "%H:%M:%S", tm_info);
                offset += snprintf(buf + offset, sizeof(buf) - offset,
                                   "%-16s %-7d %-8s %-12s %-10lu %-10lu\n",
                                   rec->id, rec->host_pid,
                                   state_to_string(rec->state), tstr,
                                   rec->soft_limit_bytes >> 20,
                                   rec->hard_limit_bytes >> 20);
                rec = rec->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            resp.status = 0;
            strncpy(resp.message, buf, sizeof(resp.message) - 1);
            break;
        }

        /* -- logs: return the log file path to the client -- */
        case CMD_LOGS: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *rec = ctx.containers;
            while (rec && strcmp(rec->id, req.container_id) != 0)
                rec = rec->next;
            if (rec) {
                resp.status = 0;
                strncpy(resp.message, rec->log_path, sizeof(resp.message) - 1);
            } else {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "no container '%s'", req.container_id);
            }
            pthread_mutex_unlock(&ctx.metadata_lock);
            break;
        }

        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "unknown command");
            break;
        }

        write(client_fd, &resp, sizeof(resp));
        close(client_fd);
    }

    /* ---- Orderly shutdown ---- */
    fprintf(stderr, "Supervisor shutting down...\n");

    /* SIGTERM all still-running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            rec->stop_requested = 1;
            kill(rec->host_pid, SIGTERM);
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait for all children */
    while (waitpid(-1, NULL, 0) > 0)
        ;

    /* Drain and stop the logging pipeline */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        container_record_t *nxt = rec->next;
        free(rec);
        rec = nxt;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "Supervisor exited cleanly.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/* CLI client — send_control_request (Step 3)                         */
/* ------------------------------------------------------------------ */

/*
 * send_control_request
 *
 * Connects to the supervisor's UNIX socket, sends a control_request_t,
 * reads back a control_response_t, and prints the result.
 *
 * For CMD_LOGS: resp.message contains the log file path.
 *              We open and print it directly rather than sending the
 *              file content over the socket.
 */
static int send_control_request(const control_request_t *req)
{
    int fd;
    struct sockaddr_un addr;
    control_response_t resp;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect — is the supervisor running?");
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    if (read(fd, &resp, sizeof(resp)) != (ssize_t)sizeof(resp)) {
        perror("read response");
        close(fd);
        return 1;
    }

    close(fd);

    /* CMD_LOGS: resp.message is a file path — read and cat it */
    if (req->kind == CMD_LOGS && resp.status == 0) {
        FILE *f = fopen(resp.message, "r");
        if (!f) {
            fprintf(stderr, "Cannot open log file: %s\n", resp.message);
            return 1;
        }
        char line[512];
        while (fgets(line, sizeof(line), f))
            fputs(line, stdout);
        fclose(f);
        return 0;
    }

    printf("%s\n", resp.message);
    return resp.status == 0 ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* CLI command handlers (unchanged from boilerplate, stubs removed)   */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)       - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)      - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/* main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}