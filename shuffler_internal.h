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
 * shuffler_internal.h  shuffler internal data structures
 * 28-Jun-2017  chuck@ece.cmu.edu
 */

/*
 * internal data structures for the 3 hop shuffler.
 */

#include <map>
#include <queue>
#include "xqueue.h"

struct req_parent;                  /* forward decl, see below */
struct outset;                      /* forward decl, see below */

/*
 * request: a structure to describe a single write request.
 * it has a fixed sized header (first four fields), and a 
 * variable length data buffer.   we always allocate the header
 * and the data together.   data will be null if datalen == 0.
 */
struct request {
  /* fields that are transmitted over the wire */
  int datalen;                      /* length of data buffer */
  int type;                         /* message type (0=normal) */
  int src;                          /* SRC rank */
  int dst;                          /* DST rank */
  void *data;                       /* request data */

  /* internal fields (not sent over the wire) */
  /* 
   * the owner is something (the application, a hg_handle_t) that
   * has been flow controlled waiting for the request to be removed 
   * a waitq.  owner can be NULL if nothing is waiting.  to change
   * a non-null owner or "next" linkage, lock the req's waitq.
   */
  struct req_parent *owner;         /* waiter that generated the request */
  XSIMPLEQ_ENTRY(request) next;     /* next request in a queue of requests */
};

/*
 * request_queue: a simple queue of request structures
 */
XSIMPLEQ_HEAD(request_queue, request);

/*
 * rpcin_t: a batch of requests (top-level RPC request structure).
 * when we serialize this, we add a request with datalen/type=zero
 * to mark the end of the list (XXX: safer that trying to use 
 * hg_proc_get_size_left()?).
 */
typedef struct {
  int32_t seq;                      /* seq# (echoed back), for debugging */
  struct request_queue inreqs;      /* list of malloc'd requests */
} rpcin_t;

/*
 * rpcout_t: return value from the server.
 */
typedef struct {
  int32_t seq;                      /* seq# (echoed back), for debugging */
  int32_t from;                     /* rank of proc sending reply */
  int32_t ret;                      /* return value */
} rpcout_t;

/*
 * req_parent: a structure to describe the owner of a group of
 * one or more waiting requests.  the owner is either the main 
 * application thread (via shuffler_send()) or it is an
 * hg_handle_t from an inbound rpc request.   we use this 
 * to track when all requests have been processed and the caller 
 * can continue or the rpc can be responded to (this is for flow 
 * control).
 *
 * "nrefs" atomically tracks the number of pending requests we are 
 * waiting on.  fields other than "nrefs" are only changed when there 
 * are no active references (either because we just allocated the 
 * req_parent or because we just released the last reference).   the 
 * exception is "ret" which can be set to something other than 
 * HG_SUCCESS on an error to give a hint on what the problem was...
 */
struct req_parent {
  hg_atomic_int32_t nrefs;          /* atomic ref counter */
  hg_return_t ret;                  /* return status (HG_SUCCESS, normally) */
  int32_t rpcin_seq;                /* saved copy of rpcin.seq */
  hg_handle_t input;                /* RPC input, or NULL for app input */
  /* next three only used if input == NULL (thus via shuffler_send()) */
  pthread_mutex_t pcvlock;          /* lock for pcv */
  pthread_cond_t pcv;               /* app may block here for flow ctl */
  int need_wakeup;                  /* need wakeup when nwaiting cleared */
  /* used when building a list of zero-ref'd req_parent's to free */
  int onfq;                         /* non-zero on an fq list (to be safe) */
  struct req_parent *fqnext;        /* free queue next */
};

/*
 * output: a single output that has been started with HG_Forward()
 * but has not yet completed (i.e. RPC request has been sent, but
 * we have not gotten the callback with the result from the remote
 * end yet...
 */
struct output {
  struct outqueue *oqp;             /* owning output queue */
  hg_handle_t outhand;              /* out handle used with HG_Forward() */
  XTAILQ_ENTRY(output) q;           /* linkage (locked by oqlock) */
};
 
/*
 * sending_outputs: a list of outputs currently being sent
 */
XTAILQ_HEAD(sending_outputs, output);

/*
 * outqueue: an output queue to a mercury endpoint (either na+sm or
 * network).  we append a request to "loading" each time we get an 
 * output until we reach our target buffer size, then we send the batch.
 */
struct outqueue {
  /* config */
  struct outset *myset;             /* output set that owns this queue */
  hg_addr_t dst;                    /* who we send to (nexus owns this) */

  pthread_mutex_t oqlock;           /* output queue lock */
  struct request_queue loading;     /* list of requests we are loading */
  int loadsize;                     /* size of loading, send when buftarget */

  struct sending_outputs outs;      /* outputs currently being sent to dst */
  int nsending;                     /* #of sends in progress for dst */

  std::queue<request *> oqwaitq;    /* if queue full, waitq of reqs */

  /* fields for flushing an output queue */
  int oqflushing;                   /* 1 if oq is flushing */
  int oqflush_waitcounter;          /* #of waitq reqs flush is waiting on */
  struct output *oqflush_output;    /* output flush is waiting on */
};

/*
 * outset: a set of local or remote output queues
 */
struct outset {
  /* config */
  int maxrpc;                       /* max# of outstanding sent RPCs */
  int buftarget;                    /* target size of an RPC (in bytes) */

  /* general state */
  shuffler_t shuf;                  /* shuffler that owns us */
  hg_class_t *mcls;                 /* mercury class */
  hg_context_t *mctx;               /* mercury context */
  hg_id_t rpcid;                    /* id of this RPC */
  int nshutdown;                    /* to signal ntask to shutdown */
  int nrunning;                     /* ntask is valid and running */
  pthread_t ntask;                  /* network thread */

  /* a map of all the output queues we known about */
  std::map<hg_addr_t,struct outqueue *> oqs;

  /* state for tracking a flush op (locked w/"flushlock") */
  int oqflushing;                   /* non-zero if flush in progress */
  hg_atomic_int32_t oqflush_counter;/* #qs flushing (hold flushlock to init) */

  /* stats (only modified/updated by ntask) */
  int nprogress;                    /* mercury progress fn counter */
  int ntrigger;                     /* mercury trigger fn counter */
};

/*
 * flush_op: a flush opearion.  may be on pending list waiting to
 * run or may be currently running.   typically stack allocated by
 * caller...   locked with flushlock.
 */
struct flush_op {
  pthread_cond_t flush_waitcv;      /* wait here for our turn or flush done */
  int status;                       /* see below */
/* status values */
#define FLUSHQ_PENDING  0           /* flush is on pending list waiting */
#define FLUSHQ_READY    1           /* flush is running */
#define FLUSHQ_CANCEL  -1           /* flush has been canceled */
  XSIMPLEQ_ENTRY(flush_op) fq; /* linkage */
};

/*
 * flush_queue: queue of flush operations (e.g. pending ops)
 */
XSIMPLEQ_HEAD(flush_queue, flush_op);

/*
 * shuffler: top-level shuffler structure
 */
struct shuffler {
  /* general config */
  nexus_ctx_t *nxp;                 /* routing table */
  char *funname;                    /* strdup'd copy of mercury func. name */
  int disablesend;                  /* disable new sends (for shutdown) */

  /* output queues */
  struct outset localq;             /* for na+sm to local procs */
  struct outset remoteq;            /* for network to remote nodes */
  hg_atomic_int32_t seqsrc;         /* source for seq# */

  /* delivery queue cfg */
  int deliverq_max;                 /* max #reqs we queue before blocking */
  shuffler_deliver_t delivercb;     /* callback function ptr */

  /* delivery thread and queue itself */
  pthread_mutex_t deliverlock;      /* locks this block of fields */
  pthread_cond_t delivercv;         /* deliver thread blocks on this */
  std::queue<request *> deliverq;   /* acked reqs being delivered */
  std::queue<request *> dwaitq;     /* unacked reqs waiting for deliver */
  int dflush_counter;               /* #of req's flush is waiting for */
  int dshutdown;                    /* to signal dtask to shutdown */
  int drunning;                     /* dtask is valid and running */
  pthread_t dtask;                  /* delivery thread */

  /* flush operation management - flush ops are serialized */
  pthread_mutex_t flushlock;        /* locks the following fields */
  struct flush_queue fpending;      /* queue of pending flush ops */
  int flushbusy;                    /* set if a flush op is in progress */
  struct flush_op *curflush;        /* currently running flush */
  int flushdone;                    /* set when current op done */
  int flushtype;                    /* current flush's type (for diag/logs) */
  struct outset *flushoset;         /* flush outset if local/remote */
/* possible flush types */
#define FLUSH_NONE    0
#define FLUSH_LOCALQ  1             /* flushing local na+sm queues */
#define FLUSH_REMOTEQ 2             /* flushing remote network queues */
#define FLUSH_DELIVER 3             /* flushing delivery queue */

};

