/*
 * Copyright (c) 2017, Carnegie Mellon University.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
 * HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * nexus-runner.cc  run deltafs-nexus and report results
 * 14-Jun-2017  chuck@ece.cmu.edu
 */

/*
 * this program tests/benchmarks the deltafs-nexus shuffle/routing
 * module.  we use MPI to managing the processes in the test.  the
 * test requires deltafs-nexus (which pulls in Mercury and MPI
 * itself).
 *
 * this is a peer-to-peer style application, so it contains both a
 * mercury RPC client and a mercury RPC server.  the client sends
 * "count" number of RPC requests via nexus to random ranks.  the
 * application exits when all requested sends have completed
 * (finished processes will wait at a MPI barrier until all sending
 * and processing has completed).
 *
 * nexus-runner was initially based on the mercury-runner
 * test/benchmark program (thus the name), but it serves a different
 * function (e.g. nexus-runner doesn't do mercury bulk ops).
 *
 * to use this program you need to launch it as an MPI application.
 * the launch process will determine the number of nodes allocated and
 * the number of processes per node.  nexus uses MPI_Comm_split_type
 * to determine the node-level configuration.  thus nexus-runner
 * itself does not have any topology configuration command line flags,
 * it uses whatever it gets from the MPI launcher.
 *
 * the shuffler queue config controls how much buffering is used and
 * how many RPCs can be active at one time.
 *
 * usage: nexus-runner [options] mercury-protocol subnet
 *
 * options:
 *  -c count     number of RPCs to perform
 *  -p baseport  base port number
 *  -q           quiet mode - don't print during RPCs
 *  -r n         enable tag suffix with this run number
 *  -t secs      timeout (alarm)
 *
 * shuffler queue config:
 *  -B bytes     batch buffer target for network output queues
 *  -b bytes     batch buffer target for shared memory output queues
 *  -d count     delivery queue limit
 *  -M count     maxrpcs for network output queues
 *  -m count     maxrpcs for shared memory output queues
 *
 * size related options:
 * -i size     input req size (> 24 if specified)
 *
 * the input reqs contain:
 *
 *  <seq,xlen,src,dest><extra bytes...>
 *
 * (so 4*sizeof(int) == 16, assuming 32 bit ints).  the "-i" flag can
 * be used to add additional un-used data to the payload if desired.
 * (this is the "xlen" --- number of extra bytes at end)
 *
 * examples:
 *
 *   ./nexus-runner -c 50 -q cci+tcp 10.92
 *
 * XXXCDC: port# handling --- maybe just add rank to base
 * XXXCDC: handle caches?
 * XXXCDC: non-ip may not be possible with nexus
 * XXXCDC: when know to exit?   (flushing)
 */

#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/resource.h>
#include <sys/time.h>
#include <sys/types.h>

#include <mercury.h>
#include <mercury_macros.h>

#include <mpi.h>   /* XXX: nexus requires this */

#include <deltafs-nexus/deltafs-nexus_api.h>

#include "shuffler.h"

/*
 * helper/utility functions, included inline here so we are self-contained
 * in one single source file...
 */
char *argv0;                     /* argv[0], program name */
int myrank = 0;

/*
 * vcomplain/complain about something.  if ret is non-zero we exit(ret)
 * after complaining.  if r0only is set, we only print if myrank == 0.
 */
void vcomplain(int ret, int r0only, const char *format, va_list ap) {
    if (!r0only || myrank == 0) {
        fprintf(stderr, "%s: ", argv0);
        vfprintf(stderr, format, ap);
        fprintf(stderr, "\n");
    }
    if (ret) {
        MPI_Finalize();
        exit(ret);
    }
}

void complain(int ret, int r0only, const char *format, ...) {
    va_list ap;
    va_start(ap, format);
    vcomplain(ret, r0only, format, ap);
    va_end(ap);
}

/*
 * start-end usage state
 */
struct useprobe {
    int who;                /* flag to getrusage */
    struct timeval t0, t1;
    struct rusage r0, r1;
};

/* load starting values into useprobe */
static void useprobe_start(struct useprobe *up, int who) {
    up->who = who;
    if (gettimeofday(&up->t0, NULL) < 0 || getrusage(up->who, &up->r0) < 0)
        complain(1, 0, "useprobe_start syscall failed?!");
}


/* load final values into useprobe */
static void useprobe_end(struct useprobe *up) {
    if (gettimeofday(&up->t1, NULL) < 0 || getrusage(up->who, &up->r1) < 0)
        complain(1, 0, "useprobe_end syscall failed?!");
}

/* print useprobe info */
void useprobe_print(FILE *out, struct useprobe *up, const char *tag, int n) {
    char nstr[32];
    double start, end;
    double ustart, uend, sstart, send;
    long nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw;

    if (n >= 0) {
        snprintf(nstr, sizeof(nstr), "%d: ", n);
    } else {
        nstr[0] = '\0';
    }

    start = up->t0.tv_sec + (up->t0.tv_usec / 1000000.0);
    end = up->t1.tv_sec + (up->t1.tv_usec / 1000000.0);

    ustart = up->r0.ru_utime.tv_sec + (up->r0.ru_utime.tv_usec / 1000000.0);
    uend = up->r1.ru_utime.tv_sec + (up->r1.ru_utime.tv_usec / 1000000.0);

    sstart = up->r0.ru_stime.tv_sec + (up->r0.ru_stime.tv_usec / 1000000.0);
    send = up->r1.ru_stime.tv_sec + (up->r1.ru_stime.tv_usec / 1000000.0);

    nminflt = up->r1.ru_minflt - up->r0.ru_minflt;
    nmajflt = up->r1.ru_majflt - up->r0.ru_majflt;
    ninblock = up->r1.ru_inblock - up->r0.ru_inblock;
    noublock = up->r1.ru_oublock - up->r0.ru_oublock;
    nnvcsw = up->r1.ru_nvcsw - up->r0.ru_nvcsw;
    nnivcsw = up->r1.ru_nivcsw - up->r0.ru_nivcsw;

    fprintf(out, "%s%s: times: wall=%f, usr=%f, sys=%f (secs)\n", nstr, tag,
        end - start, uend - ustart, send - sstart);
    fprintf(out,
      "%s%s: minflt=%ld, majflt=%ld, inb=%ld, oub=%ld, vcw=%ld, ivcw=%ld\n",
      nstr, tag, nminflt, nmajflt, ninblock, noublock, nnvcsw, nnivcsw);
}

/*
 * getsize: a souped up version of atoi() that handles suffixes like
 * 'k' (so getsize("1k") == 1024).
 */
int64_t getsize(char *from) {
    int len, end;
    int64_t rv;

    len = strlen(from);
    if (len == 0)
        return(0);
    rv = atoi(from);
    end = tolower(from[len-1]);
    switch (end) {    /* ordered to fallthrough */
        case 'g':
            rv = rv * 1024;
        case 'm':
            rv = rv * 1024;
        case 'k':
            rv = rv * 1024;
    }

    return(rv);
}

/*
 * end of helper/utility functions.
 */

/*
 * default values for port and count
 */
#define DEF_BASEPORT 19900 /* starting TCP port we listen on (instance 0) */
#define DEF_BUFTARGET 1    /* target #bytes for a batch */
#define DEF_COUNT 5        /* default # of msgs to send and recv in a run */
#define DEF_DELIVERQMAX 1  /* max# of reqs in deliverq before using waitq */
#define DEF_MAXRPCS 1      /* max# of outstanding RPCs */
#define DEF_TIMEOUT 120    /* alarm timeout */

/*
 * gs: shared global data (e.g. from the command line)
 */
struct gs {
    int ninst;               /* currently locked at 1 */
    /* note: MPI rank stored in global "myrank" */
    int size;                /* world size (from MPI) */
    char *hgproto;           /* hg protocol to use */
    char *hgsubnet;          /* subnet to use (XXX: assumes IP) */
    int baseport;            /* base port number */
    int buftarg_net;         /* batch target for network queues */
    int buftarg_shm;         /* batch target for shared memory queues */
    int count;               /* number of msgs to send/recv in a run */
    int deliverq_max;        /* max# reqs in deliverq before waitq */
    int maxrpcs_net;         /* max # outstanding RPCs, network */
    int maxrpcs_shm;         /* max # outstanding RPCs, shared memory */
    int quiet;               /* don't print so much */
    int rflag;               /* -r tag suffix spec'd */
    int rflagval;            /* value for -r */
    int timeout;             /* alarm timeout */
    char tagsuffix[64];      /* tag suffix: ninst-count-mode-limit-run# */

    /*
     * inreq size includes bytes used for seq,xlen,src,dest.
     * if is zero then we just have those four numbers.  otherwise
     * it must be >= 40 to account for the header (we pad the rest).
     */
    int inreqsz;             /* input request size */
} g;

/*
 * is: per-instance state structure.   currently we only allow
 * one instance per proc (but we keep this broken out in case
 * we want to change it...).
 */
struct is {
    int n;                   /* our instance number (0 .. n-1) */
    nexus_ctx_t *nxp;        /* nexus context */
    char myfun[64];          /* my function name */
    shuffler_t shand;        /* shuffler handler */
    int nsends;              /* number of times we've called send */
    int ncallbacks;          /* #times our callback was called */
};
struct is *isa;    /* an array of state */

/*
 * alarm signal handler
 */
void sigalarm(int foo) {
    int lcv;
    fprintf(stderr, "SIGALRM detected (%d)\n", myrank);
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        fprintf(stderr, "%d: %d: @alarm: ", myrank, lcv);
        fprintf(stderr, "nsends=%d, ncallbacks=%d\n",
                isa[lcv].nsends, isa[lcv].ncallbacks);
    }
    fprintf(stderr, "Alarm clock\n");
    MPI_Finalize();
    exit(1);
}

/*
 * usage
 */
static void usage(const char *msg) {

    /* only have rank 0 print usage error message */
    if (myrank) goto skip_prints;

    if (msg) fprintf(stderr, "%s: %s\n", argv0, msg);
    fprintf(stderr, "usage: %s [options] mercury-protocol [subnet]\n", argv0);
    fprintf(stderr, "\noptions:\n");
    fprintf(stderr, "\t-c count    number of RPCs to perform\n");
    fprintf(stderr, "\t-p port     base port number\n");
    fprintf(stderr, "\t-q          quiet mode\n");
    fprintf(stderr, "\t-r n        enable tag suffix with this run number\n");
    fprintf(stderr, "\t-t sec      timeout (alarm), in seconds\n");
    fprintf(stderr, "\nuse '-l 1' to serialize RPCs\n\n");
    fprintf(stderr, "shuffler queue config:\n");
    fprintf(stderr, "\t-B bytes    batch buf target for network\n");
    fprintf(stderr, "\t-b bytes    batch buf target for shm\n");
    fprintf(stderr, "\t-d count    delivery queue size limit\n");
    fprintf(stderr, "\t-M count    maxrpcs for network output queues\n");
    fprintf(stderr, "\t-m count    maxrpcs for shm output queues\n");
    fprintf(stderr, "\nsize related options:\n");
    fprintf(stderr, "\t-i size     input req size (>= 24 if specified)\n");
    fprintf(stderr, "\ndefault payload size is 24.\n");

skip_prints:
    MPI_Finalize();
    exit(1);
}

/*
 * forward prototype decls.
 */
static void *run_instance(void *arg);   /* run one instance */

/*
 * main program.  usage:
 *
 * ./nexus-runner [options] mercury-protocol subnet
 */
int main(int argc, char **argv) {
    struct timeval tv;
    int ch, lcv, rv;
    pthread_t *tarr;
    struct useprobe mainuse;
    char mytag[128];

    argv0 = argv[0];

    /* mpich says we should call this early as possible */
    if (MPI_Init(&argc, &argv) != MPI_SUCCESS) {
        fprintf(stderr, "%s: MPI_Init failed.  MPI is required.\n", argv0);
        exit(1);
    }

    /* we want lines, even if we are writing to a pipe */
    setlinebuf(stdout);

    /* init random for random data */
    (void)gettimeofday(&tv, NULL);
    srandom(getpid() + tv.tv_sec);

    /* setup default to zero/null, except as noted below */
    memset(&g, 0, sizeof(g));
    if (MPI_Comm_rank(MPI_COMM_WORLD, &myrank) != MPI_SUCCESS)
        complain(1, 0, "unable to get MPI rank");
    if (MPI_Comm_size(MPI_COMM_WORLD, &g.size) != MPI_SUCCESS)
        complain(1, 0, "unable to get MPI size");
    g.baseport = DEF_BASEPORT;
    g.buftarg_net = DEF_BUFTARGET;
    g.buftarg_shm = DEF_BUFTARGET;
    g.count = DEF_COUNT;
    g.deliverq_max = DEF_DELIVERQMAX;
    g.maxrpcs_net = DEF_MAXRPCS;
    g.maxrpcs_shm = DEF_MAXRPCS;
    g.timeout = DEF_TIMEOUT;

    while ((ch = getopt(argc, argv, "B:b:c:d:i:M:m:p:qr:t:")) != -1) {
        switch (ch) {
            case 'B':
                g.buftarg_net = atoi(optarg);
                if (g.buftarg_net < 1) usage("bad buftarget net");
                break;
            case 'b':
                g.buftarg_shm = atoi(optarg);
                if (g.buftarg_shm < 1) usage("bad buftarget shm");
                break;
            case 'c':
                g.count = atoi(optarg);
                if (g.count < 1) usage("bad count");
                break;
            case 'd':
                g.deliverq_max = atoi(optarg);
                if (g.deliverq_max < 1) usage("bad deliverq_max shm");
                break;
            case 'i':
                g.inreqsz = getsize(optarg);
                if (g.inreqsz <= 16) usage("bad inreqsz (must be > 16)");
                break;
            case 'M':
                g.maxrpcs_net = atoi(optarg);
                if (g.maxrpcs_net < 1) usage("bad maxrpc net");
                break;
            case 'm':
                g.maxrpcs_shm = atoi(optarg);
                if (g.maxrpcs_shm < 1) usage("bad maxrpc shm");
                break;
            case 'p':
                g.baseport = atoi(optarg);
                if (g.baseport < 1) usage("bad port");
                break;
            case 'q':
                g.quiet = 1;
                break;
            case 'r':
                g.rflag++;  /* will gen tag suffix after args parsed */
                g.rflagval = atoi(optarg);
                break;
            case 't':
                g.timeout = atoi(optarg);
                if (g.timeout < 0) usage("bad timeout");
                break;
            default:
                usage(NULL);
        }
    }
    argc -= optind;
    argv += optind;

    if (argc != 2)          /* hgproto and hgsubnet must be provided on cli */
      usage("bad args");
    g.ninst = 1;
    g.hgproto = argv[0];
    g.hgsubnet = argv[1];
    if (g.rflag) {
        snprintf(g.tagsuffix, sizeof(g.tagsuffix), "-%d-%d",
                 g.count, g.rflagval);
    }

    if (myrank == 0) {
        printf("\n%s options:\n", argv0);
        printf("\tMPI_rank   = %d\n", myrank);
        printf("\tMPI_size   = %d\n", g.size);
        printf("\thgproto    = %s\n", g.hgproto);
        printf("\thgsubnet   = %s\n", g.hgsubnet);
        printf("\tbaseport   = %d\n", g.baseport);
        printf("\tcount      = %d\n", g.count);
        printf("\tquiet      = %d\n", g.quiet);
        if (g.rflag)
            printf("\tsuffix     = %s\n", g.tagsuffix);
        printf("\ttimeout    = %d\n", g.timeout);
        printf("sizes:\n");
        printf("\tbuftarget  = %d / %d (net/shm)\n", g.buftarg_net,
               g.buftarg_shm);
        printf("\tmaxrpcs    = %d / %d (net/shm)\n", g.maxrpcs_net,
               g.maxrpcs_shm);
        printf("\tdeliverqmx = %d\n", g.deliverq_max);
        printf("\tinput      = %d\n", (g.inreqsz == 0) ? 4 : g.inreqsz);
        printf("\n");
    }

    signal(SIGALRM, sigalarm);
    alarm(g.timeout);
    if (myrank == 0) printf("main: starting ...\n");

    tarr = (pthread_t *)malloc(g.ninst * sizeof(pthread_t));
    if (!tarr) complain(1, 0, "malloc tarr thread array failed");
    isa = (struct is *)malloc(g.ninst *sizeof(*isa));    /* array */
    if (!isa) complain(1, 0, "malloc 'isa' instance state failed");
    memset(isa, 0, g.ninst * sizeof(*isa));

    /* fork off a thread for each instance */
    useprobe_start(&mainuse, RUSAGE_SELF);
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        isa[lcv].n = lcv;
        rv = pthread_create(&tarr[lcv], NULL, run_instance, (void*)&isa[lcv]);
        if (rv != 0)
            complain(1, 0, "pthread create failed %d", rv);
    }

    /* now wait for everything to finish */
    if (myrank == 0) printf("main: collecting\n");
    for (lcv = 0 ; lcv < g.ninst ; lcv++) {
        pthread_join(tarr[lcv], NULL);
    }
    useprobe_end(&mainuse);

    if (myrank == 0) printf("main: collection done.\n");
    snprintf(mytag, sizeof(mytag), "ALL%s", g.tagsuffix);
    useprobe_print(stdout, &mainuse, mytag, -1);

    MPI_Barrier(MPI_COMM_WORLD);
    if (myrank == 0) printf("main exiting...\n");

    MPI_Finalize();
    exit(0);
}

/*
 * run_instance: the main routine for running one instance of mercury.
 * we pass the instance state struct in as the arg...
 */
void *run_instance(void *arg) {
    struct is *isp = (struct is *)arg;
    int n = isp->n;               /* recover n from isp */
    nexus_ret_t nrv;
    int lcv;

    printf("%d: instance running\n", myrank);
    isa[n].n = n;    /* make it easy to map 'is' structure back to n */
    isa[n].nxp = new nexus_ctx_t;   /* XXXCDC: need ctor to run */

    /* XXXCDC: port stuff likely to go away */
    nrv = nexus_bootstrap(isp->nxp, g.baseport, g.baseport+1000 /*XXX*/,
                          g.hgsubnet, g.hgproto);
    if (nrv != NX_SUCCESS)
        complain(1, 0, "%d: nexus_bootstrap failed: %d", myrank, nrv);
    printf("%d: nexus powered up!\n", myrank);

    /* make a funcion name and register it in both HGs */
    snprintf(isa[n].myfun, sizeof(isa[n].myfun), "f%d", n);

    isa[n].shand = shuffler_init(isa[n].nxp, isa[n].myfun, g.maxrpcs_shm,
                   g.buftarg_shm, g.maxrpcs_net, g.buftarg_net,
                   g.deliverq_max, NULL/*XXXCDC CALLBACK */);

#if 0
    /* XXXCDC: need callback */
    /*
     * for lcv = 0 ; lcv < g.count ;lcv++
     *   send data to a random end point
     *
     */
#endif
    return(NULL);
}

#if 0
/*
 * OLD STUFF OLD STUFF OLD STUFF
 */
#include <assert.h>
#include <errno.h>
#include <inttypes.h>
#include <sys/stat.h>

/*
 * run_instance: the main routine for running one instance of mercury.
 * we pass the instance state struct in as the arg...
 */
void *run_instance(void *arg) {
#if 0
    int lcv, rv;
    char *remoteurl;
    hg_return_t ret;
    struct lookup_state lst;
    hg_op_id_t lookupop;
    struct useprobe rp;
    struct callstate *cs;
    unsigned char data;
#endif

#if 0
    /* fork off network progress/trigger thread */
    is[n].sends_done = 0;   /* run_network reads this */
    rv = pthread_create(&is[n].nthread, NULL, run_network, (void*)&n);
    if (rv != 0) complain(1, "pthread create srvr failed %d", rv);

    if (g.mode != MR_SERVER) {    /* plain server-only can start right away */
        /* poor man's barrier */
        printf("%d: init done.  sleeping 10\n", n);
        sleep(10);
    }

    /*
     * resolve the remote address for client ... only need to do this
     * once, since it is fixed for this program...
     */
    if (g.mode & MR_CLIENT) {
        remoteurl = (g.dir) ? load_dir_addr(n) : is[n].remoteid;
        printf("%d: remote address lookup %s\n", n, remoteurl);
        if (pthread_mutex_init(&lst.lock, NULL) != 0)
            complain(1, "lst.lock mutex init");
        pthread_mutex_lock(&lst.lock);
        lst.n = n;
        lst.done = 0;
        if (pthread_cond_init(&lst.lkupcond, NULL) != 0)
            complain(1, "lst.lkupcond cond init");

        ret = HG_Addr_lookup(is[n].hgctx, lookup_cb, &lst,
                             remoteurl, &lookupop);
        if (ret != HG_SUCCESS) complain(1, "HG addr lookup launch failed");
        while (lst.done == 0) {
            if (pthread_cond_wait(&lst.lkupcond, &lst.lock) != 0)
                complain(1, "lst.lkupcond cond wait");
        }
        if (lst.done < 0) complain(1, "lookup failed");
        pthread_cond_destroy(&lst.lkupcond);
        pthread_mutex_unlock(&lst.lock);
        pthread_mutex_destroy(&lst.lock);
        if (remoteurl != is[n].remoteid) free(remoteurl);
        remoteurl = NULL;
        printf("%d: done remote address lookup\n", n);

        /* poor man's barrier again... */
        printf("%d: address lookup done.  sleeping 10 again\n", n);
        sleep(10);
    }

#ifdef RUSAGE_THREAD
    useprobe_start(&rp, RUSAGE_THREAD);
#else
    useprobe_start(&rp, RUSAGE_SELF);
#endif

    if (g.mode == MR_SERVER) {
        printf("%d: server mode, skipping send step\n", n);
        goto skipsend;
    }

    printf("%d: sending...\n", n);
    if (pthread_mutex_init(&is[n].slock, NULL) != 0)
        complain(1, "slock mutex init");
    is[n].nsent = 0;
    is[n].scond_mode = SM_OFF;
    if (pthread_cond_init(&is[n].scond, NULL) != 0) complain(1, "scond init");
    /* starting lcv at 1, indicates number we are sending */
    for (lcv = 1 ; lcv <= g.count ; lcv++) {

        cs = get_callstate(&is[n]);  /* from free list or freshly malloc'd */

        cs->in.seq = (g.extend_rpcin) ? (lcv | RPC_EXTENDED) : lcv;
        if (g.extend_rpcin && cs->rd_rmabuf) {
            data = random();
            *((char *)cs->rd_rmabuf) = data;  /* data for sanity check */
            if (!g.quiet)
                printf("%d: prelaunch %d: set data to %d\n", n, lcv, data);
        }


        if (!g.quiet)
            printf("%d: launching %d\n", n, lcv);
        ret = HG_Forward(cs->callhand, forw_cb, cs, &cs->in);
        is[n].nstarted++;
        if (ret != HG_SUCCESS) complain(1, "hg forward failed");
        if (!g.quiet)
            printf("%d: launched %d (size=%d)\n", n, lcv, (int)cs->in.sersize);

        /* flow control */
        pthread_mutex_lock(&is[n].slock);
        /* while in-flight >= limit */
        while ((lcv - is[n].nsent) >= g.limit) {
            is[n].scond_mode = SM_SENTONE;      /* as soon as room is there */
            if (pthread_cond_wait(&is[n].scond, &is[n].slock) != 0)
                complain(1, "client send flow control cond wait");
        }
        pthread_mutex_unlock(&is[n].slock);
    }

    /* wait until all sends are complete (already done if serialsend) */
    pthread_mutex_lock(&is[n].slock);
    while (is[n].nsent < g.count) {
        is[n].scond_mode = SM_SENTALL;
        if (pthread_cond_wait(&is[n].scond, &is[n].slock) != 0)
            complain(1, "snd cond wait");
    }
    pthread_cond_destroy(&is[n].scond);
    pthread_mutex_unlock(&is[n].slock);
    pthread_mutex_destroy(&is[n].slock);
    is[n].sends_done = 1;
    printf("%d: all sends complete\n", n);

skipsend:
    /* done sending, wait for network thread to finish and exit */
    pthread_join(is[n].nthread, NULL);
    if (is[n].remoteaddr) {
        HG_Addr_free(is[n].hgclass, is[n].remoteaddr);
        is[n].remoteaddr = NULL;
    }
    useprobe_end(&rp);
    printf("%d: all recvs complete\n", n);

    /* dump the callstate cache */
    while ((cs = is[n].cfree) != NULL) {
        is[n].cfree = cs->next;
        free_callstate(cs);
    }
    is[n].ncfree = 0;     /* just to be clear */

    printf("%d: destroy context and finalize mercury\n", n);
    HG_Context_destroy(is[n].hgctx);
    HG_Finalize(is[n].hgclass);

    if (g.mode & MR_CLIENT) {
        double rtime = (rp.t1.tv_sec + (rp.t1.tv_usec / 1000000.0)) -
                       (rp.t0.tv_sec + (rp.t0.tv_usec / 1000000.0));
        printf("%d: client%s: %d rpc%s in %f sec (%f sec per op)\n",
               n, g.tagsuffix, g.count, (g.count == 1) ? "" : "s",
               rtime, rtime / (double) g.count);
    }

#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &rp, "instance", n);
#endif
#endif /* XXX: IF 0 */
    delete is[n].nxp;
    printf("%d.%d: instance done\n", myrank, n);
    return(NULL);
}
#if 0

/*
 * forw_cb: this gets called on the client side when HG_Forward() completes
 * (i.e. when we get the reply from the remote side).
 */
static hg_return_t forw_cb(const struct hg_cb_info *cbi) {
    struct callstate *cs = (struct callstate *)cbi->arg;
    hg_handle_t hand;
    struct is *isp;
    hg_return_t ret;
    rpcout_t out;
    int oldmode;
    unsigned char data;

    if (cbi->ret != HG_SUCCESS) complain(1, "forw_cb failed");
    if (cbi->type != HG_CB_FORWARD) complain(1, "forw_cb wrong type");
    hand = cbi->info.forward.handle;
    if (hand != cs->callhand) complain(1, "forw_cb mismatch hands");
    isp = cs->isp;

    ret = HG_Get_output(hand, &out);
    if (ret != HG_SUCCESS) complain(1, "get output failed");

    if (!g.quiet) {
        if (cs->wr_rmabuf) {
            data = *((char *)cs->wr_rmabuf);
            printf("%d: forw complete (code=%d,reply_size=%d, data=%d)\n",
                   isp->n, ~out.ret & RPC_SEQMASK, (int)out.sersize,
                   data);
        } else {
            printf("%d: forw complete (code=%d,reply_size=%d)\n",
                   isp->n, ~out.ret & RPC_SEQMASK, (int)out.sersize);
        }
    }

    HG_Free_output(hand, &out);

    /* update records and see if we need to signal client */
    pthread_mutex_lock(&isp->slock);
    isp->nsent++;
    if (isp->scond_mode != SM_OFF) {
        oldmode = isp->scond_mode;
        if (oldmode == SM_SENTONE || isp->nsent >= g.count) {
            isp->scond_mode = SM_OFF;
            pthread_cond_signal(&isp->scond);
        }
    }

    /* either put cs in cache for reuse or free it */
    if (g.xcallcachemax < 0 ||
        (g.xcallcachemax != 0 && isp->ncfree >= g.xcallcachemax)) {

        free_callstate(cs);    /* get rid of it */

    } else {

        cs->next = isp->cfree; /* cache for reuse */
        isp->cfree = cs;
        isp->ncfree++;

    }
    cs = NULL;

    pthread_mutex_unlock(&isp->slock);

    return(HG_SUCCESS);
}

/*
 * run_network: network support pthread.   need to call progress to push the
 * network and then trigger to run the callback.  we do this all in
 * one thread (meaning that we shouldn't block in the trigger function,
 * or we won't make progress).  since we only have one thread running
 * trigger callback, we do not need to worry about concurrent access to
 * "got" ...
 */
static void *run_network(void *arg) {
    int n = *((int *)arg);
#ifdef RUSAGE_THREAD
    struct useprobe rn;
#endif
    unsigned int actual;
    hg_return_t ret;
    struct respstate *rs;
    is[n].recvd = is[n].responded = actual = 0;
    is[n].nprogress = is[n].ntrigger = 0;

    printf("%d: network thread running\n", n);
#ifdef RUSAGE_THREAD
    useprobe_start(&rn, RUSAGE_THREAD);
#endif

    /* while (not done sending or not done recving */
    while ( ((g.mode & MR_CLIENT) && !is[n].sends_done  ) ||
            ((g.mode & MR_SERVER) && is[n].responded < g.count) ) {

        do {
            ret = HG_Trigger(is[n].hgctx, 0, 1, &actual);
            is[n].ntrigger++;
        } while (ret == HG_SUCCESS && actual);

        /* recheck, since trigger can change is[n].got */
        if (!is[n].sends_done || is[n].responded < g.count) {
            HG_Progress(is[n].hgctx, 100);
            is[n].nprogress++;
        }

    }

    /* dump the respstate cache */
    while ((rs = is[n].rfree) != NULL) {
        is[n].rfree = rs->next;
        free_respstate(rs);
    }
    is[n].nrfree = 0;     /* just to be clear */

#ifdef RUSAGE_THREAD
    useprobe_end(&rn);
#endif
    printf("%d: network thread complete (nprogress=%d, ntrigger=%d)\n", n,
           is[n].nprogress, is[n].ntrigger);
#ifdef RUSAGE_THREAD
    useprobe_print(stdout, &rn, "net", n);
#endif
    return(NULL);
}

#endif
/*
 * server side funcions....
 */

/*
 * rpchandler: called on the server when a new RPC comes in
 */
static hg_return_t rpchandler(hg_handle_t handle) {
#if 0
    struct is *isp;
    const struct hg_info *hgi;
    struct respstate *rs;
    hg_return_t ret;
    int32_t inseq;

    /* gotta extract "isp" using handle, 'cause that's the only way pass it */
    hgi = HG_Get_info(handle);
    if (!hgi) complain(1, "rpchandler: bad hgi");
    isp = (struct is *) HG_Registered_data(hgi->hg_class, hgi->id);
    if (!isp) complain(1, "rpchandler: bad isp");

    /* currently safe: only one network thread and we are in it */
    isp->recvd++;

    rs = get_respstate(isp);
    rs->callhand = handle;
    ret = HG_Get_input(handle, &rs->in);
    if (ret != HG_SUCCESS) complain(1, "rpchandler: HG_Get_input failed");

    inseq = rs->in.seq & RPC_SEQMASK;
    if (!g.quiet)
        printf("%d: got remote input %d (size=%d)\n", isp->n, inseq,
               (int)rs->in.sersize);

    rs->out.ret = ~inseq & RPC_SEQMASK;
    if (g.extend_rpcout)
        rs->out.ret |= RPC_EXTENDED;

    rs->phase = RS_READCLIENT;

    ret = advance_resp_phase(rs);

    return(ret);
#endif
    return(HG_SUCCESS); //XXXCDC
}

#if 0
/*
 * advance_resp_phase: push the rs forward
 */
static hg_return_t advance_resp_phase(struct respstate *rs) {
    const struct hg_info *hgi;
    hg_size_t tomove;
    hg_return_t rv;
    hg_op_id_t dummy;
    int32_t inseq;
    unsigned char data;

    hgi = HG_Get_info(rs->callhand);  /* to get remote's host address */
    if (!hgi)
        complain(1, "advance_resp_phase: HG_Get_info failed?");

 again:

    switch (rs->phase) {

    case RS_READCLIENT:
        rs->phase++;
        if (rs->in.nread == 0 || rs->in.bread == HG_BULK_NULL)
            goto again;    /* nothing to read from client, move on */
        if (g.blrmasz == 0) {
            complain(0, "advance_resp_phase: no lbuf to rma read in (skip)");
            goto again;
        }
        tomove = rs->in.nread;
        if (g.blrmasz < tomove) {
            complain(0, "advance_resp_phase: lbuf too small, trunc by %d",
                     (int)tomove - g.blrmasz);
            tomove = g.blrmasz;

        }
        if (!g.quiet)
            printf("%d: %d: starting RMA read %" PRId64 " bytes\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, tomove);

        rv = HG_Bulk_transfer(rs->isp->hgctx, reply_bulk_cb, (void *)rs,
                              HG_BULK_PULL, hgi->addr, rs->in.bread,
                              0, rs->lrmabufhand, 0, tomove, &dummy);

        if (rv != HG_SUCCESS)
            complain(1, "HG_Bulk_tranfer failed? (%d)", rv);
        break;

    case RS_WRITECLIENT:
        rs->phase++;
        if (rs->in.nwrite == 0 || rs->in.bwrite == HG_BULK_NULL)
            goto again;   /* nothing to write to client, move on */
        if (g.blrmasz == 0) {
            complain(0, "advance_resp_phase: no lbuf to rma write in (skip)");
            goto again;
        }
        tomove = rs->in.nwrite;
        if (g.blrmasz < tomove) {
            complain(0, "advance_resp_phase: lbuf too small, trunc by %d",
                     (int)tomove - g.blrmasz);
            tomove = g.blrmasz;

        }
        data = random();
        *((char *)rs->lrmabuf) = data;  /* data for sanity check */
        if (!g.quiet)
            printf("%d: %d: starting RMA write %" PRId64 " bytes, data=%d\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, tomove, data);

        rv = HG_Bulk_transfer(rs->isp->hgctx, reply_bulk_cb, (void *)rs,
                              HG_BULK_PUSH, hgi->addr, rs->in.bwrite,
                              0, rs->lrmabufhand, 0, tomove, &dummy);

        if (rv != HG_SUCCESS)
            complain(1, "HG_Bulk_tranfer failed? (%d)", rv);

        break;

    default:   /* must be RS_RESPOND */
        inseq = rs->in.seq & RPC_SEQMASK;
        rv = HG_Free_input(rs->callhand, &rs->in);

        /* the callback will bump "got" after respond has been sent */
        rv = HG_Respond(rs->callhand, reply_sent_cb, rs, &rs->out);
        if (rv != HG_SUCCESS) complain(1, "rpchandler: HG_Respond failed");
        if (!g.quiet)
            printf("%d: responded to %d (size=%d)\n", rs->isp->n, inseq,
                   (int)rs->out.sersize);

    }

    return(HG_SUCCESS);
}

/*
 * reply_bulk_cb: called after the server completes a bulk op
 */
static hg_return_t reply_bulk_cb(const struct hg_cb_info *cbi) {
    struct respstate *rs;
    struct is *isp;
    int oldphase;
    unsigned char data;

    if (cbi->type != HG_CB_BULK)
        complain(1, "reply_bulk_cb:unexpected sent cb");

    rs = (struct respstate *)cbi->arg;
    isp = rs->isp;
    oldphase = rs->phase - 1;

    if (oldphase == RS_READCLIENT) {
        data = *((char *)rs->lrmabuf);
        if (!g.quiet)
            printf("%d: %d: server bulk read from client complete (data=%d)\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK, data);
    } else {
        if (!g.quiet)
            printf("%d: %d server bulk write to client complete\n",
                   rs->isp->n, rs->in.seq & RPC_SEQMASK);
    }

    return(advance_resp_phase(rs));
}


/*
 * reply_sent_cb: called after the server's reply to an RPC completes.
 */
static hg_return_t reply_sent_cb(const struct hg_cb_info *cbi) {
    struct respstate *rs;
    struct is *isp;

    if (cbi->type != HG_CB_RESPOND)
        complain(1, "reply_sent_cb:unexpected sent cb");

    rs = (struct respstate *)cbi->arg;
    isp = rs->isp;

    /*
     * currently safe: there is only one network thread and we
     * are in it (via trigger fn).
     */
    isp->responded++;

    if (cbi->info.respond.handle != rs->callhand)
        complain(1, "reply_send_cb sanity check failed");

    /* return handle to the pool for reuse */
    HG_Destroy(rs->callhand);
    rs->callhand = NULL;

    /* either put rs in cache for reuse or free it */
    if (g.yrespcachemax < 0 ||
        (g.yrespcachemax != 0 && isp->ncfree >= g.yrespcachemax)) {

        free_respstate(rs);    /* get rid of it */

    } else {

        rs->next = isp->rfree; /* cache for reuse */
        isp->rfree = rs;
        isp->nrfree++;

    }

    return(HG_SUCCESS);
}
#endif
#endif
