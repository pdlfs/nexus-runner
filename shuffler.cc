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
 * shuffler.cc  3 hop shuffle code
 * 28-Jun-2017  chuck@ece.cmu.edu
 */

#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include <mercury.h>
#include <mercury_atomic.h>
#include <mercury_macros.h>
#include <mpi.h> /*XXX: deltafs-nexus_api.h needs MPI_Comm */
#include <deltafs-nexus/deltafs-nexus_api.h>

#include "shuffler.h"
#include "shuffler_internal.h"

/*
 * quick reminder:
 *
 * forwarding path:
 *    SRC -na+sm-> SRCREP -network-> DSTREP -na+sm-> DST
 *
 * steps can be skipped (e.g. if SRC == SRCREP, skip the first na+sm hop).
 *
 * 3 threads: na+sm mercury thread, network mercury thread, delivery thread
 * (the final hop is always via the delivery thread)
 *
 * "send" vs "forward":
 *  - applications "send" a single request via the shuffler_send() API
 *  - shuffler "forwards" RPCs received from shuffler_rpchand() callback,
 *    each RPC contains a batch of one or more requests
 * 
 * write back buffering handled by allowing requests to complete when
 * placed in a queue.  if the queue is full, request go in a wait queue
 * and either the app is blocked ("send") or the RPC reply is delayed
 * ("forward").  this is managed with the req_parent structure.
 */

/*
 * RPC handler registered with mercury
 */
static hg_return_t shuffler_rpchand(hg_handle_t handle);

/*
 * thread main routines for network and delivery
 */
static void *delivery_main(void *arg);
static void *network_main(void *arg);

/*
 * other prototypes
 */

static bool append_req_to_locked_outqueue(struct outset *oset, 
                                          struct outqueue *oq,
                                          struct request *req,
                                          struct request_queue *tosendq,
                                          bool flushnow);
static hg_return_t aquire_flush(struct shuffler *sh, struct flush_op *fop,
                                int type, struct outset *oset);
static void clean_qflush(struct shuffler *sh, struct outset *oset);
static void done_oq_flush(struct outqueue *oq);
static void drop_curflush(struct shuffler *sh);
static hg_return_t forw_cb(const struct hg_cb_info *cbi);
static void forw_start_next(struct outqueue *oq, struct output *oput);
static hg_return_t forward_reqs_now(struct request_queue *tosendq, 
                                    struct shuffler *sh, struct outset *oset,
                                    struct outqueue *oq);
static int purge_reqs(struct shuffler *sh);
static int purge_reqs_outset(struct shuffler *sh, int local);
static hg_return_t req_parent_init(struct req_parent **parentp, 
                                   struct request *req, hg_handle_t input, 
                                   int32_t rpcin_seq);
static hg_return_t req_to_self(struct shuffler *sh, struct request *req, 
                               hg_handle_t input, int32_t in_seq, 
                               struct req_parent **parentp);
static hg_return_t req_via_mercury(struct shuffler *sh, struct outset *oset,
                                   struct outqueue *oq, struct request *req,
                                   hg_handle_t input, int32_t in_seq,
                                   struct req_parent **parentp);
static void parent_dref_stopwait(struct shuffler *sh, struct req_parent *parent,
                                 int abort);
static void parent_stopwait(struct shuffler *sh, struct req_parent *parent,
                            int abort);
static hg_return_t shuffler_desthand_cb(const struct hg_cb_info *cbi);
static hg_return_t shuffler_respond_cb(const struct hg_cb_info *cbi);
static int start_threads(struct shuffler *sh);
static void stop_threads(struct shuffler *sh);
static int start_qflush(struct shuffler *sh, struct outset *oset, 
                        struct outqueue *oq);

/*
 * functions used to serialize/deserialize our RPCs args (e.g. XDR-like fn).
 */

static int32_t zero = 0;   /* for end of list marker */

/* 
 * procheck: helper macro to reduce the verbage ... 
 */
#define procheck(R,MSG) if ((R) != HG_SUCCESS) { \
    hg_log_write(HG_LOG_TYPE_ERROR, "HG", __FILE__, __LINE__, __func__, MSG); \
    goto done; \
}

/*
 * hg_proc_rpcin_t: encode/decode the rpcin_t structure
 *
 * @param proc the proc used to serialize/deserialize the data
 * @param data pointer to the data being worked on
 * @return HG_SUCCESS or an error code
 */
static hg_return_t hg_proc_rpcin_t(hg_proc_t proc, void *data) {
  hg_return_t ret = HG_SUCCESS;
  hg_proc_op_t op = hg_proc_get_op(proc);
  rpcin_t *struct_data = (rpcin_t *) data;
  struct request *rp, *nrp;
  int lcv;
  int32_t dlen, typ;

  if (op == HG_FREE)               /* we combine free and err handling below */
    goto done;

  if (op == HG_DECODE) {           /* start with an empty inreqs list */
    XSIMPLEQ_INIT(&struct_data->inreqs);
  }

  ret = hg_proc_hg_int32_t(proc, &struct_data->seq);
  procheck(ret, "Proc err seq");

  if (op == HG_ENCODE) {   /* serialize list to the proc */
    XSIMPLEQ_FOREACH(rp, &struct_data->inreqs, next) {
      ret = hg_proc_hg_int32_t(proc, &rp->datalen);
      procheck(ret, "Proc en err datalen");
      ret = hg_proc_hg_int32_t(proc, &rp->type);
      procheck(ret, "Proc en err type");
      ret = hg_proc_hg_int32_t(proc, &rp->src);
      procheck(ret, "Proc en err src");
      ret = hg_proc_hg_int32_t(proc, &rp->dst);
      procheck(ret, "Proc en err dst");
      ret = hg_proc_memcpy(proc, rp->data, rp->datalen);
      procheck(ret, "Proc en err data");
    }
    /* put in the end of list marker */
    for (lcv = 0 ; lcv < 4 ; lcv++) {
      ret = hg_proc_hg_int32_t(proc, &zero);
      procheck(ret, "Proc err zero");
    }
    goto done;
  }

  /* op == HG_DECODE */
  while (1) {
    ret = hg_proc_hg_int32_t(proc, &dlen);  /* should err if we use up data */
    procheck(ret, "Proc de err datalen");
    ret = hg_proc_hg_int32_t(proc, &typ);
    procheck(ret, "Proc de err type");
    if (dlen == 0 && typ == 0) break;     /* got end of list marker */
    rp = (request*)malloc(sizeof(*rp) + dlen);
    if (rp == NULL) ret = HG_NOMEM_ERROR;
    procheck(ret, "Proc de malloc");
    rp->datalen = dlen;
    rp->type = typ;
    ret = hg_proc_hg_int32_t(proc, &rp->src);
    if (ret == HG_SUCCESS) ret = hg_proc_hg_int32_t(proc, &rp->dst);
    rp->data = ((char *)rp) + sizeof(*rp);
    if (ret == HG_SUCCESS) ret = hg_proc_memcpy(proc, rp->data, dlen);
    rp->owner = NULL;
    if (ret != HG_SUCCESS) {
      free(rp);
      procheck(ret, "Proc decoder");
    }
 
    /* got it!  put at the end of the decoded list */
    XSIMPLEQ_INSERT_TAIL(&struct_data->inreqs, rp, next);
  }

done:
  if ( ((op == HG_DECODE && ret != HG_SUCCESS) || op == HG_FREE) && 
       XSIMPLEQ_FIRST(&struct_data->inreqs) != NULL) {
    XSIMPLEQ_FOREACH_SAFE(rp, &struct_data->inreqs, next, nrp) {
      free(rp);
    }
    XSIMPLEQ_INIT(&struct_data->inreqs);
  }
  return(ret);
}

/*
 * hg_proc_rpcout_t: encode/decode the rpcout_t structure
 *
 * @param proc the proc used to serialize/deserialize the data
 * @param data pointer to the data being worked on
 * @return HG_SUCCESS or an error code
 */
static hg_return_t hg_proc_rpcout_t(hg_proc_t proc, void *data) {
    hg_return_t ret = HG_SUCCESS;
    /* hg_proc_op_t op = hg_proc_get_op(proc); */  /* don't need it */
    rpcout_t *struct_data = (rpcout_t *) data;

    ret = hg_proc_hg_int32_t(proc, &struct_data->seq);
    procheck(ret, "Proc err seq");
    ret = hg_proc_hg_int32_t(proc, &struct_data->from);
    procheck(ret, "Proc err src");
    ret = hg_proc_hg_int32_t(proc, &struct_data->ret);
    procheck(ret, "Proc err ret");

done:
    return(ret);
}

/*
 * shuffler_outset_discard: free anything that was attached to an outset
 * (e.g. for error recovery, shutdown)
 * 
 * @param oset the outset to clean
 */
static void shuffler_outset_discard(struct outset *oset) {
  std::map<hg_addr_t,struct outqueue *>::iterator oqit;
  struct outqueue *oq;

  oqit = oset->oqs.begin();
  while (oqit != oset->oqs.end()) {
    oq = oqit->second;
    pthread_mutex_destroy(&oq->oqlock);
    delete oq;
  }
}

/*
 * shuffler_init_outset: init an outset (but does not start network thread)
 *
 * @param oset the structure we are init'ing
 * @param maxrpc max# of outstanding RPCs allowed
 * @param buftarget try and collect at least this many bytes into batch
 * @param shuf the shuffler that owns this oset
 * @param mcls mercury class
 * @param mctx mercury context
 * @param nmap map of valid addresses (from nexus)
 * @param rpchand rpc handler function (we register it)
 * @return -1 on error, 0 on success
 */
static int shuffler_init_outset(struct outset *oset, int maxrpc, int buftarget,
                                shuffler_t shuf, hg_class_t *mcls,
                                hg_context_t *mctx, nexus_map_t *nmap,
                                hg_rpc_cb_t rpchand) {
  std::map<int,hg_addr_t>::iterator nmit;
  hg_addr_t ha;
  struct outqueue *oq;

  oset->maxrpc = maxrpc;
  oset->buftarget = buftarget;
  oset->shuf = shuf;
  oset->mcls = mcls;
  oset->mctx = mctx;
  /* save rpcid for the end */
  oset->nshutdown = 0;
  oset->nrunning = 0;     /* this indicates that ntask is not valid/init'd */
  /* oqs init'd by ctor */
  oset->nprogress = 0;
  oset->ntrigger = 0;

  /* now populate the oqs */
  for (nmit = nmap->begin() ; nmit != nmap->end() ; nmit++) {
    ha = nmit->second;
    oq = new struct outqueue;
    if (!oq) goto err;
    oq->myset = oset;
    oq->dst = ha;         /* shared with nexus, nexus owns it */
    if (pthread_mutex_init(&oq->oqlock, NULL) != 0) {
      delete oq;
      goto err;
    }
    XSIMPLEQ_INIT(&oq->loading);
    XTAILQ_INIT(&oq->outs);
    oq->loadsize = oq->nsending = 0;
    oq->oqflushing = oq->oqflush_waitcounter = 0;
    oq->oqflush_output = NULL;

    /* waitq init'd by ctor */
    oset->oqs[ha] = oq;    /* map insert, malloc's under the hood */
  }

  /* finally we add it to mercury */
  /* XXX: HG_Register_name can't fail? */
  /* XXX: no api to unregister an RPC other than shutting down mercury */
  oset->rpcid = HG_Register_name(mcls, shuf->funname, 
                 hg_proc_rpcin_t, hg_proc_rpcout_t,  rpchand);
  if (HG_Register_data(mcls, oset->rpcid, oset, NULL) != HG_SUCCESS)
    goto err;
  return(0);

err:
  shuffler_outset_discard(oset);
  return(-1);
}

/*
 * shuffler_flush_discard: discard allocated state for flush mgt
 *
 * @param sh shuffler previously init'd with shuffler_init_flush
 */
static void shuffler_flush_discard(struct shuffler *sh) {
  int nc = 0;
  struct flush_op *fop;

  /* kill any pending flush ops (hopefully none) */
  pthread_mutex_lock(&sh->flushlock);
  while ((fop = XSIMPLEQ_FIRST(&sh->fpending)) != NULL) {
    XSIMPLEQ_REMOVE_HEAD(&sh->fpending, fq);
    fop->status = FLUSHQ_CANCEL;
    pthread_cond_signal(&fop->flush_waitcv);
    nc++;
  }

  if (sh->flushbusy && sh->curflush) {
    sh->curflush->status = FLUSHQ_CANCEL;
    pthread_cond_signal(&sh->curflush->flush_waitcv);
    nc++;
  }
  pthread_mutex_unlock(&sh->flushlock);

  if (nc) {
    fprintf(stderr, "shuffler: flush_discard canceled %d flush op(s)\n", nc);
    sleep(3);   /* yield to be safe since destroy flushlock is next */
  }
  pthread_mutex_destroy(&sh->flushlock);
}

/*
 * shuffler_init_flush: init flush op management fields in shuffler
 *
 * @param sh shuffler to init
 * @return success, normally
 */
static hg_return_t shuffler_init_flush(struct shuffler *sh) {
  sh->flushbusy = 0;
  XSIMPLEQ_INIT(&sh->fpending);
  sh->curflush = NULL;
  sh->flushdone = 0;
  sh->flushtype = FLUSH_NONE;
  sh->flushoset = NULL;

  if (pthread_mutex_init(&sh->flushlock, NULL) != 0)
    return(HG_NOMEM_ERROR);
  return(HG_SUCCESS);
}

/*
 * shuffler_init: init's the shuffler layer.
 */
shuffler_t shuffler_init(nexus_ctx_t *nxp, char *funname,
           int lmaxrpc, int lbuftarget, int rmaxrpc, int rbuftarget,
           int deliverq_max, shuffler_deliver_t delivercb) {
  shuffler_t sh;
  int rv;

  sh = new shuffler;    /* aborts w/std::bad_alloc on failure */
  sh->nxp = nxp;
  sh->funname = strdup(funname);
  if (!sh->funname)
    goto err;
  sh->disablesend = 0;

  rv = shuffler_init_outset(&sh->localq, lmaxrpc, lbuftarget, sh, 
          nxp->local_hgcl, nxp->local_hgctx, &nxp->laddrs, /* XXX: layering */
          shuffler_rpchand);
  if (rv < 0) goto err;

  rv = shuffler_init_outset(&sh->remoteq, rmaxrpc, rbuftarget, sh, 
          nxp->remote_hgcl, nxp->remote_hgctx, &nxp->gaddrs, /* XXX: layering */
          shuffler_rpchand);
  if (rv < 0) goto err;
  hg_atomic_set32(&sh->seqsrc, 0);

  sh->deliverq_max = deliverq_max;
  sh->delivercb = delivercb;
  if (pthread_mutex_init(&sh->deliverlock, NULL) != 0)
    goto err;
  if (pthread_cond_init(&sh->delivercv, NULL) != 0) {
    pthread_mutex_destroy(&sh->deliverlock);
    goto err;
  }
  sh->dflush_counter = 0;
  sh->dshutdown = sh->drunning = 0;

  if (shuffler_init_flush(sh) != HG_SUCCESS) {
    pthread_mutex_destroy(&sh->deliverlock);
    pthread_cond_destroy(&sh->delivercv);
    goto err;
  }

  /* now start our three worker threads */
  if (start_threads(sh) != 0) {
    pthread_mutex_destroy(&sh->deliverlock);
    pthread_cond_destroy(&sh->delivercv);
    shuffler_flush_discard(sh);
    goto err;
  }

  return(sh);

err:
  shuffler_outset_discard(&sh->localq);     /* ensures maps are empty */
  shuffler_outset_discard(&sh->remoteq);  
  if (sh->funname) free(sh->funname);
  delete sh;
  return(NULL);
}

/*
 * start_threads: attempt to start our three worker threads
 * 
 * @param sh the shuffler we are starting
 * @return 0 on success, -1 on error
 */
static int start_threads(struct shuffler *sh) {
  int rv;

  /* start delivery thread */
  rv = pthread_create(&sh->dtask, NULL, delivery_main, (void *)sh);
   if (rv != 0) {
     fprintf(stderr, "shuffler:start_threads: delivery_main failed\n");
     stop_threads(sh);
     return(-1);
   }
   sh->drunning = 1;

   /* start local na+sm thread */
  rv = pthread_create(&sh->localq.ntask, NULL, 
                      network_main, (void *)&sh->localq);
  if (rv != 0) {
     fprintf(stderr, "shuffler:start_threads: na+sm main failed\n");
     stop_threads(sh);
     return(-1);
  }
  sh->localq.nrunning = 1;

   /* start remote network thread */
  rv = pthread_create(&sh->remoteq.ntask, NULL, 
                      network_main, (void *)&sh->remoteq);
  if (rv != 0) {
     fprintf(stderr, "shuffler:start_threads: net main failed\n");
     stop_threads(sh);
     return(-1);
  }
  sh->remoteq.nrunning = 1;

  return(0);
}

/*
 * stop_threads: stop all our worker threads.  this will prevent
 * any requests from progressing, so clear out the queues.
 *
 * @param sh shuffler
 */
static void stop_threads(struct shuffler *sh) {
  int stranded;

  /* stop network */
  if (sh->remoteq.nrunning) {
    sh->remoteq.nshutdown = 1;
    pthread_join(sh->remoteq.ntask, NULL);
    sh->remoteq.nshutdown = 0;
  }

  /* stop na+sm */
  if (sh->localq.nrunning) {
    sh->localq.nshutdown = 1;
    pthread_join(sh->localq.ntask, NULL);
    sh->localq.nshutdown = 0;
  }

  /* stop delivery */
  if (sh->drunning) {
    sh->dshutdown = 1;
    pthread_join(sh->dtask, NULL);
    sh->dshutdown = 0;
  }

  /* look for stranded requests and warn about them */
  stranded = purge_reqs(sh);
  if (stranded > 0) {
    fprintf(stderr, "shuffler:stop_threads: WARNING - stranded %d reqs\n",
            stranded);
  }
}

/*
 * purge_reqs: we've stopped the network and delivery so no more 
 * progress is going to be made.  look for reqs that are still in 
 * the system and clear them out.  return number of reqs we cleared 
 * (hopefully zero).
 *
 * @param sh the shuffler to purge
 * @return number of items that got purged
 */
static int purge_reqs(struct shuffler *sh) {
  int rv = 0;
  struct request *req;

  if (sh->drunning || sh->localq.nrunning || sh->remoteq.nrunning) {
    fprintf(stderr, "ERROR!  purge_reqs called on active system?!!?\n");
    abort();   /* should never happen */
  }

  /* clear delivery queues */
  while (!sh->dwaitq.empty()) {
    req = sh->dwaitq.front();
    sh->dwaitq.pop();
    parent_dref_stopwait(sh, req->owner, 1);
    free(req);
    rv++;
  }
  while (!sh->deliverq.empty()) {
    req = sh->dwaitq.front();
    sh->dwaitq.pop();
    free(req);
    rv++;
  }

  /* clear local and remote queeus */
  rv += purge_reqs_outset(sh, 1);
  rv += purge_reqs_outset(sh, 0);

  return(rv);
}

/*
 * purge_reqs_outset: helper function purge_reqs() that clears an outset.
 * the threads should have been stopped prior to running this (so we are
 * not expecting concurrent access while we are tearing this down).
 * 
 * @param sh the shuffler oset belongs to
 * @param local set to select localq, otherwise remoteq
 * @return the number of stranded reqs in the outset
 */
static int purge_reqs_outset(struct shuffler *sh, int local) {
  int rv = 0;
  struct outset *oset;
  std::map<hg_addr_t,struct outqueue *>::iterator it;
  struct outqueue *oq;
  struct request *req, *nxt;
  struct output *oput;

  oset = (local) ? &sh->localq : &sh->remoteq;

  /* need to purge each output queue in the set */
  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

   /* stop flushing */
   if (oq->oqflushing) {
     oq->oqflushing = 0;
     oq->oqflush_waitcounter = 0;
     oq->oqflush_output = NULL;
     hg_atomic_decr32(&oset->oqflush_counter);
   }

   /* zap the wait queue */
    while (!oq->oqwaitq.empty()) {
      req = oq->oqwaitq.front();
      oq->oqwaitq.pop();
      parent_dref_stopwait(sh, req->owner, 1);
      free(req);
      rv++;
    }

    /* now zap the loading requests */
    XSIMPLEQ_FOREACH_SAFE(req, &oq->loading, next, nxt) {
      free(req);
      rv++;
    }

    /* and dump the requests in progress */
    while ((oput = XTAILQ_FIRST(&oq->outs)) != NULL) {
      XTAILQ_REMOVE(&oq->outs, oput, q);
      /* 
       * XXX: what to do with outhand.  should we cancel it?  threads
       * are not running.   seems like we hold a ref we should drop
       * at any rate.
       */
      HG_Destroy(oput->outhand);   
      free(oput);
    }
  }

  oset->oqflushing = 0;
  
  return(rv);
}

/*
 * delivery_main: main routine for delivery thread.  the delivery
 * thread does final delivery of messages to the application (via
 * the delivery callback).   we need this thread because the final
 * delivery can block (e.g. for flow control) and we don't want to
 * block our network threads because of it (since it would stop
 * traffic that we are a REP for).
 *
 * @param arg void* pointer to our shuffler
 */
static void *delivery_main(void *arg) {
  struct shuffler *sh = (struct shuffler *)arg;
  struct request *req;
  struct req_parent *parent;

  pthread_mutex_lock(&sh->deliverlock);
  while (sh->dshutdown == 0) {
    if (sh->deliverq.empty()) {
      (void)pthread_cond_wait(&sh->delivercv, &sh->deliverlock);
      continue;
    }

    /* 
     * start first entry of the queue -- this may block, so unlock 
     * to allow other threads to append to the queues.   note that
     * this is the only thread that dequeues reqs from deliverq, so
     * it is safe to leave req at the front while we are running the 
     * callback...
     */
    req = sh->deliverq.front();
    if (!req) abort();   /* shouldn't ever happen */

    pthread_mutex_unlock(&sh->deliverlock);
    sh->delivercb(req->src, req->dst, req->type, req->data, req->datalen);
    pthread_mutex_lock(&sh->deliverlock);
 
    /* see if anyone is waiting for us to flush */
    if (sh->dflush_counter > 0) {
      sh->dflush_counter--;
      if (sh->dflush_counter == 0) {   /* droped to 0, wake up flusher */
        if (sh->curflush) 
          pthread_cond_signal(&sh->curflush->flush_waitcv);
      }
    }
    
    /* dispose of the req we just delivered */
    sh->deliverq.pop();
    if (req->owner)        /* should never happen */
      fprintf(stderr, "delivery_main: freeing req with owner!?!\n");
    free(req);
    req = NULL;

    /* just made space in deliveryq, see if we can advance one from waitq */
    if (sh->dwaitq.empty())
      continue;                 /* waitq empty, loop back up */

    /* move it to deliveryq */
    req = sh->dwaitq.front();
    sh->dwaitq.pop();
    sh->deliverq.push(req);     /* deliverq should be full again */

    /* 
     * now we need to tell req's parent it can stop waiting.  since
     * we are holding the deliver lock (covers the dwaitq) we can
     * clear the owner to detach the req from the parent.   then
     * we need to call parent_dref_stopwait() to drop the parent's 
     * reference counter.
     *
     * XXX: be safe and drop deliverlock when calling parent_dref_stopwait().
     * normally parent_dref_stopwait() will just drop the reference count and
     * if it drops to zero it will call HG_Reply (if parent->input !NULL) 
     * pthread_cond_signal (if parent->input == NULL).  the main worry
     * is HG_Reply() since that code is external to us and we can't
     * know what it (or any mercury NA layer under it) will do.
     */
    parent = req->owner;
    req->owner = NULL;
    pthread_mutex_unlock(&sh->deliverlock);
    parent_dref_stopwait(sh, parent, 0);
    pthread_mutex_lock(&sh->deliverlock);
  }
  sh->drunning = 0;
  pthread_mutex_unlock(&sh->deliverlock);

  return(NULL);
}

/*
 * parent_dref_stopwait: drop parent reference count and stop waiting 
 * if we've dropped the last reference
 *
 * @param sh our shuffler
 * @param parent waiting parent of request that is no longer waiting
 * @param abort cancels further processing of parent (e.g. for error handling)
 */
static void parent_dref_stopwait(struct shuffler *sh, struct req_parent *parent,
                                 int abort) {
  int nw;

  if (parent == NULL) {
    /* this should never happen */
    fprintf(stderr, "parent_dref_stopwait: ERROR - waiting req w/no parent\n");
    return;
  }

  /* atomically drop the reference counter and get new value */
  nw = hg_atomic_decr32(&parent->nrefs);

  if (nw > 0) {   /* still active reqs, let it keep waiting ... */
    return;
  }

  parent_stopwait(sh, parent, abort);
}


/*
 * parent_stopwait: we had a req on a waitq (i.e. a parent waiting on it)
 * and it finished.  we removed the req from the waitq and dropped the 
 * ref count.   the ref count has dropped to zero, so we need to unblock
 * the parent and set it up for disposal.
 *
 * NOTE: caller must ensure that assume that nrefs is < 1  
 *
 * @param sh our shuffler
 * @param parent waiting parent of request that is no longer waiting
 * @param abort cancels further processing of parent (e.g. for error handling)
 */
static void parent_stopwait(struct shuffler *sh, struct req_parent *parent, 
                            int abort) {
  rpcout_t reply;
  hg_return_t rv;

  if (parent == NULL) {
    /* this should never happen */
    fprintf(stderr, "stopwait: ERROR - waiting req w/no parent\n");
    return;
  }

  /* 
   * parent no longer has any active refs besides us, so no thread other
   * than us can access it anymore (meaning it is safe to access it 
   * without additional locking).
   */

  /* set a non-success code if we are aborting ... */
  if (abort)
    parent->ret = HG_CANCELED;

  /*
   * we've launched the last waiting request, so now the parent no
   * longer needs to wait.   we have two types of parents: the
   * application process or an input hg_handle_t.
   *
   * if the parent is the application process (input==NULL), it is 
   * waiting on parent->pcv and needs to be woken up (this can only 
   * happen when sending with SRC == DST and the app is flow controlled).
   * the application will free the req_parent.
   */
  if (parent->input == NULL) {
    pthread_mutex_lock(&parent->pcvlock);
    parent->need_wakeup = 0;   /* XXX: needed?  vs. nrefs==0 */
    pthread_cond_signal(&parent->pcv);
    pthread_mutex_unlock(&parent->pcvlock);
    return;
  }

  /*
   * ok, the parent is a flow controlled hg_handle_t that we can 
   * now respond to.   once we've stopped the wait, we can dispose 
   * of the req_parent (we have the only reference to it now, so we 
   * can just free it).
   */
  reply.seq = parent->rpcin_seq;
  reply.from = sh->nxp->grank; /* XXX: layering */
  reply.ret = parent->ret;

  /* only respond if we are not aborting */
  if (!abort) {
    rv = HG_Respond(parent->input, shuffler_respond_cb, parent, &reply);
  } else {
    rv = HG_CANCELED;
  }

  if (rv != HG_SUCCESS) {
    struct hg_cb_info cbi;   /* fake, for err/abort! */
    if (!abort)
      fprintf(stderr, "shuffler_stopwaiting: HG_Respond failed %d?\n", rv);
    /* note: we know shuffler_respond_cb() only looks at cbi.arg */
    cbi.arg = parent;
    rv = shuffler_respond_cb(&cbi);
    /* ignore return value */
  }
}

/*
 * shuffler_respond_cb: we've either send a response or we tried
 * to send a response and failed.  either way, we need to do final
 * cleanup.  note that we only use the field cbi->arg in cbi (in
 * the failure case we are getting a fake cbi, see above).
 *
 * @param cbi the arg for the callback
 * @return success
 */
static hg_return_t shuffler_respond_cb(const struct hg_cb_info *cbi) {
  struct req_parent *parent = (struct req_parent *)cbi->arg;

  /*
   * XXX: if we want to cache a list of free parent structures, 
   * we could do it here...
   */

  HG_Destroy(parent->input);
  free(parent);

  return(HG_SUCCESS);
}

/*
 * network_main: network support pthread.   need to call progress to 
 * push the network and then trigger to run the callback.  we do this 
 * all in one thread (meaning that we shouldn't block in the trigger 
 * function, or we won't make progress)
 *
 * @param arg void* pointer to our outset
 */

static void *network_main(void *arg) {
  struct outset *oset = (struct outset *)arg;
  hg_return_t ret;
  unsigned int actual;

  while (oset->nshutdown == 0) {

    do {
      ret = HG_Trigger(oset->mctx, 0, 1, &actual); /* triggers all callbacks */
      oset->ntrigger++;
    } while (ret == HG_SUCCESS && actual);

    HG_Progress(oset->mctx, 100);
    oset->nprogress++;

  }

  oset->nrunning = 0;
  return(NULL);
}

/*
 * req_parent_init: helper function called when we need to attach a
 * req to an init'd req_parent structure.   if the structure is already
 * init'd, we just bump the req_parent's reference count and set the 
 * req's owner.   otherwise, we are starting a new req_parent.
 * this happens when we send a req on a full queue and need to wait 
 * on a waitq or on dwaitq.  it also happens when we recv an inbound 
 * RPC handle that contains a req that needs to wait on a waitq or on 
 * dwaitq (e.g. before doing HG_Respond()).
 *
 * note: "parentp" is a pointer to a pointer.  if *parentp is NULL, we
 * will malloc a new req_parent structure.
 *
 * @param parentp ptr to ptr to the req_parent to init
 * @param req the request that we are waiting on
 * @param input inbound RPC handle (NULL if we are an app shuffler_send())
 * @param rpcin_seq inbound seq# (only used if input != NULL)
 * @return status (normally success)
 */
static hg_return_t req_parent_init(struct req_parent **parentp, 
                                   struct request *req, hg_handle_t input, 
                                   int32_t rpcin_seq) {
  struct req_parent *parent;
  int did_malloc = 0;

  parent = *parentp;

  /* can just bump nrefs for RPCs w/previously allocated parent */
  if (input && parent) {
    hg_atomic_incr32(&parent->nrefs);  /* just add a reference */
    req->owner = parent;
    return(HG_SUCCESS);
  }

  if (parent == NULL) {
    if (input == NULL) {  /* "send" ops should provide this for us */
      fprintf(stderr, "shuffler: req_parent_init usage error\n");
      return(HG_INVALID_PARAM);  /* should never happen */
    }
    parent = (struct req_parent *)malloc(sizeof(*parent));
    if (parent == NULL) {
      return(HG_NOMEM_ERROR);
    }
    *parentp = parent;
    did_malloc = 1;
  }

  /*
   * we only need pcv/pcvlock if we are in a shuffler_send() op.
   * if input is !NULL, then we are working on responding to an 
   * inbound RPC call...
   */
  if (input == NULL) {
    /* set up mutex/cv */
    if (pthread_mutex_init(&parent->pcvlock, NULL)) {  /* only for pcv */
      if (did_malloc) free(parent);
      return(HG_OTHER_ERROR);
    }
    if (pthread_cond_init(&parent->pcv, NULL)) {
      pthread_mutex_destroy(&parent->pcvlock);
      if (did_malloc) free(parent);
      return(HG_OTHER_ERROR);
    }
  }

  /*
   * we always set the initial value for nrefs to 2.  one for the 
   * req we are adding, and one for our caller in order to hold 
   * the req_parent in memory until we are completely done with it.
   * (want to avoid unlikely case where RPC completes before we
   * start waiting for the result..)
   */
  hg_atomic_set32(&parent->nrefs, 2);
  parent->ret = HG_SUCCESS;
  parent->rpcin_seq = rpcin_seq;
  parent->input = input;
  parent->need_wakeup = (input == NULL) ? 1 : 0;
  parent->onfq = 0;
  parent->fqnext = NULL;    /* to be safe */

  /* parent now owns req */
  req->owner = parent;

  return(HG_SUCCESS);
}



/*
 * shuffler_send: start the sending of a message via the shuffle.
 */
hg_return_t shuffler_send(shuffler_t sh, int dst, int type,
                          void *d, int datalen) {
  nexus_ret_t nexus;
  int rank;
  hg_addr_t dstaddr;
  struct request *req;
  struct req_parent parent_store, *parent;
  hg_return_t rv;
  struct outset *oset;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;

  /* first, check to see if send is generally disabled */
  if (sh->disablesend)
    return(HG_OTHER_ERROR);

  /* determine next hop */
  nexus = nexus_next_hop(sh->nxp, dst, &rank, &dstaddr);

  /* 
   * we always have to malloc and copy the data from the user to one
   * of our buffers because we return to the sender before the is 
   * complete (and we don't want to sender to reuse the buffer before
   * we are done with it).  
   *
   * XXX: for output queues that have room, it would be nice if we
   * could directly copy into their hg_handle_t buffer as we receive
   * new requests until the hg_handle_t is full and ready to be 
   * send, but mercury doesn't give us an API to do that (we've got
   * HG_Forward() which takes an unpacked set of requests and packs
   * them all at once... there is no way to incrementally add data).
   */
  req = (struct request *) malloc(sizeof(*req) + datalen);
  if (req == NULL) 
    return(HG_NOMEM_ERROR);

  req->datalen = datalen;
  req->type = type;
  req->src = sh->nxp->grank;  /* XXX: layering */
  req->dst = dst;
  req->data = (char *)req + sizeof(*req);
  memcpy(req->data, d, datalen);    /* DATA COPY HERE */
  req->owner = NULL;
  req->next.sqe_next = NULL;        /* to be safe */

  /* case 1: sending to ourselves */
  if (nexus == NX_DONE || req->src == dst) {

    parent = &parent_store;
    rv = req_to_self(sh, req, NULL, 0, &parent);  /* can block */
    return(rv);
  }

  /* case 2: not for us, sending request over mercury */

  /*
   * we are the SRC.  possible sub-cases:
   *  NX_ISLOCAL: dst is on local machine, use na+sm to send it
   *  NX_SRCREP: dst is remote, use na+sm to send to remote's SRCREP
   *  NX_DESTREP: dst is remote, we are SRCREP, send over network
   */
  if (nexus != NX_ISLOCAL && nexus != NX_SRCREP && nexus != NX_DESTREP) {
    /* nexus doesn't know dst, return error */
    return(HG_INVALID_PARAM);
  }

  /* need to find correct output queue for dstaddr */
  oset = (nexus == NX_DESTREP) ? &sh->remoteq : &sh->localq;
  it = oset->oqs.find(dstaddr);
  if (it == oset->oqs.end()) {
    /* 
     * nexus knew the addr, but we couldn't find a a queue!
     * this should not happen!!!
     */
    return(HG_INVALID_PARAM);
  }

  oq = it->second;    /* now we have the correct output queue */

  parent = &parent_store;
  rv = req_via_mercury(sh, oset, oq, req, NULL, 0, &parent);  /* can block */

  return(rv);
}

/*
 * req_to_self: sending/forward a req to ourself via the delivery thread.
 *
 * for apps sending (i.e. input==NULL, we are called via shuffler_send()) 
 * we will block if the delivery queue is full.  for blocking we'll use a 
 * req_parent's condvar provided by the caller to block.  the caller
 * typically allocates the req_parent on the stack.
 *
 * for reqs generated by the shuffler_rpchand() callback function
 * (input != NULL), if we get put on a wait queue we'll let 
 * req_parent_init() malloc a req_parent to save the input handle
 * on until.   we'll hold the HG_Respond() call until all reqs in
 * the input batch have cleared the waitqs.  
 *
 * as noted in req_parent_init(), freshly malloc'd req_parent structures 
 * have their reference count set to 2 (one for the RPC and one for us
 * to hold the structure in memory until we've launched everything).
 *
 * all this blocking structure is in place to support write back 
 * buffering with flow control in all cases.
 *
 * rules: if input is NULL, then this is part of a "send" operation
 * and the caller provides a req_parent (typically stack allocated) 
 * in parentp.  otherwise, if input is not NULL then this is part of 
 * a "forward" operation and a req_parent is malloc'd on demand and 
 * placed in parentp.
 *
 * if this fails, we free the request (what else can we do?) which
 * means it gets dropped ...
 *
 * @param sh the shuffler involved
 * @param req the request to send/forward to self
 * @param input the inbound handle that generated the req
 * @param in_seq the rcpin.seq value of the inbound req
 * @param parentp parent ptr (will allocate a new one if needed)
 * @return status 
 */
static hg_return_t req_to_self(struct shuffler *sh, struct request *req,
                               hg_handle_t input, int32_t in_seq, 
                               struct req_parent **parentp) {
  hg_return_t rv = HG_SUCCESS;
  int qsize, needwait;
  struct req_parent *parent;

  pthread_mutex_lock(&sh->deliverlock);
  qsize = sh->deliverq.size();
  needwait = (qsize >= sh->deliverq_max); /* wait if no room in deliverq */

  if (!needwait) {

    /* easy!  just queue and wake delivery thread (if needed) */
    sh->deliverq.push(req);
    if (qsize == 0)
      pthread_cond_signal(&sh->delivercv);  /* empty->!empty: wake thread */

  } else {

    /* sad!  we need to block on the waitq for delivery ... */
    rv = req_parent_init(parentp, req, input, in_seq);

    if (rv == HG_SUCCESS) {
      sh->dwaitq.push(req);     /* add req to wait queue */
    } else {
      fprintf(stderr, "shuffler: req_to_self parent init failed (%d)\n", rv);
      free(req);                /* error means we can't send it */
      req = NULL;               /* to be safe */
    }

  }
  pthread_mutex_unlock(&sh->deliverlock);  
  
  /*
   * if we are sending (!input) and need to wait, we'll block here.
   */
  if (!input && needwait && rv == HG_SUCCESS) {   /* wait now if needed */
    parent = *parentp;

    pthread_mutex_lock(&parent->pcvlock);
    /* drop extra parent ref created by req_parent_init() before waiting */
    hg_atomic_decr32(&parent->nrefs);
    while (hg_atomic_get32(&parent->nrefs) > 0) {
      pthread_cond_wait(&parent->pcv, &parent->pcvlock);  /*BLOCK HERE*/
    }
    pthread_mutex_unlock(&parent->pcvlock);

    /*
     * we are done now, since the thread that woke us up also 
     * should have pulled our req off the dwaitq and put it in 
     * the delivery queue.
     */

    pthread_cond_destroy(&parent->pcv);
    pthread_mutex_destroy(&parent->pcvlock);
  }

  /* done! */
  return(rv);
}

/*
 * req_via_mercury: send a req via mercury.  as usual there are two
 * cases: input == NULL: app sending directly via shuffler_send()
 *        input != NULL: forwarding req recv'd via mercury RPC
 *
 * flow control blocking is handled the same way as req_to_self()
 * (see discussion above).
 *
 * @param sh the shuffler we are sending with
 * @param oset the output queue set we are using
 * @param oq the output queue to use
 * @param req the request to send
 * @param input input RPC handle (null if via app shuffler_send call)
 * @param in_seq if input!=NULL, seq of inbound RPC msg
 * @param parentp parent ptr (will allocate a new one if needed)
 * @return status, normally success
 */
static hg_return_t req_via_mercury(struct shuffler *sh, struct outset *oset,
                                   struct outqueue *oq, struct request *req,
                                   hg_handle_t input, int32_t in_seq,
                                   struct req_parent **parentp) {
  hg_return_t rv = HG_SUCCESS;
  int needwait;
  bool tosend;
  struct request_queue tosendq;
  struct req_parent *parent;

  pthread_mutex_lock(&oq->oqlock);
  needwait = (oq->nsending >= oset->maxrpc);
  tosend = NULL;

  if (!needwait) {

    /* we can start sending this req now, no need to wait */
    tosend = append_req_to_locked_outqueue(oset, oq, req, &tosendq, false);

  } else {

    /* sad!  we need to block on the output queue till it clears some */
    rv = req_parent_init(parentp, req, input, in_seq);

    if (rv == HG_SUCCESS) {
      oq->oqwaitq.push(req);      /* add req to oq's waitq */
    } else {
      fprintf(stderr, "shuffler: req_via_mercury parent init failed (%d)\n", 
              rv);
      free(req);                /* error means we can't send it */
      req = NULL;               /* to be safe */
    }
  }
  pthread_mutex_unlock(&oq->oqlock);

  if (tosend) {   /* have a batch ready to send? */

    rv = forward_reqs_now(&tosendq, sh, oset, oq);

  } else if (!input && needwait && rv == HG_SUCCESS) { /* wait now if needed */
    parent = *parentp;

    pthread_mutex_lock(&parent->pcvlock);
    /* drop extra parent ref created by req_parent_init() before waiting */
    hg_atomic_decr32(&parent->nrefs);
    while (hg_atomic_get32(&parent->nrefs) > 0) {
      pthread_cond_wait(&parent->pcv, &parent->pcvlock);  /* BLOCK HERE */
    }
    pthread_mutex_unlock(&parent->pcvlock);

    /*
     * we are done now, since the thread that woke us up also
     * should have pulled our req off the waitq and set it up
     * for sending.
     */

    pthread_cond_destroy(&parent->pcv);
    pthread_mutex_destroy(&parent->pcvlock);
  }

  /* done! */
  return(rv);
}

/*
 * append_req_to_locked_outqueue: append a req to a locked output 
 * queue.  this may result in a message that we need to forward
 * (e.g. if we fill a batch or if we are flushing).  we bump nsending
 * if this function returns a list of reqs to send.  note that 
 * req is allowed to be NULL (e.g. if we just want to flush).
 *
 * @param oset the output set that our outq belongs to
 * @param oq the locked output queue (we've already checked for room)
 * @param req the request to append to the queue (NULL is ok)
 * @param tosend a queue of requests ready to send (OUT, if ret true)
 * @param flushnow don't wait for buftarget bytes, flush now
 * @return true a list of requests to send is in "tosend"
 */
static bool append_req_to_locked_outqueue(struct outset *oset, 
                                          struct outqueue *oq,
                                          struct request *req,
                                          struct request_queue *tosend,
                                          bool flushnow) {
  struct request *rv;

  /* first append req to the loading list */
  if (req) {
    XSIMPLEQ_INSERT_TAIL(&oq->loading, req, next);
    oq->loadsize += req->datalen;  /* add to total batch size */
  }

  /*
   * if there is still room in the batch and we are not flushing now,
   * then we can return success now!
   */
  if (oq->loadsize < oset->buftarget && !flushnow) {
    return(false);
  }

  /*
   * bump nsending, pass back list of reqs to send, and reset loading
   * list...
   */
  XSIMPLEQ_INIT(tosend);
  XSIMPLEQ_CONCAT(tosend, &oq->loading);
  /* note: "CONCAT" re-init's &oq->loading to empty */
  oq->loadsize = 0;
  oq->nsending++;

  return(true);
}

/*
 * forward_reqs_now: actually send a batch of requests now.  oq->nsending
 * has already been bumped up (we'll bump it back down on error).
 *
 * @param tosend a list of reqs to send
 * @param sh shuffler we are sending with 
 * @param oset the output queue set we are working with
 * @param oq the output queue we are sending on
 * @return status (hopefully success)
 */
static hg_return_t forward_reqs_now(struct request_queue *tosend, 
                                    struct shuffler *sh, struct outset *oset,
                                    struct outqueue *oq) {
  rpcin_t in;
  struct output *oput;
  hg_return_t rv = HG_SUCCESS;
  struct request *rp, *nrp;

  /* always rehome the requests to in */
  XSIMPLEQ_INIT(&in.inreqs);
  XSIMPLEQ_CONCAT(&in.inreqs, tosend);

  oput = (struct output *) malloc(sizeof(*oput));
  if (oput) {
    oput->oqp = oq;
    rv = HG_Create(oset->mctx, oq->dst, oset->rpcid, &oput->outhand);
    if (rv != HG_SUCCESS) {
      free(oput);
      oput = NULL;
    } else {
      pthread_mutex_lock(&oq->oqlock);
      XTAILQ_INSERT_TAIL(&oq->outs, oput, q);
      pthread_mutex_unlock(&oq->oqlock);
    }
  }

  /* 
   * if (oput != NULL) then we have a handle and we are on the outs list 
   *                   else no handle, not on the outs list
   */

  if (oput != NULL) {
    in.seq = hg_atomic_incr32(&sh->seqsrc);
    rv = HG_Forward(oput->outhand, forw_cb, oput, &in);
  }

  /* data copied to handle or we failed to send.  either way free this. */
  XSIMPLEQ_FOREACH_SAFE(rp, &in.inreqs, next, nrp) {
    free(rp);
  }

  if (oput == NULL || rv != HG_SUCCESS) { /* setup failed || HG_Forw failed */
    /*
     * this is pretty terrible... we've failed to forward our
     * batch packet.  there is no pretty way to recover from this,
     * so let's complain loudly that we've dropped data :(
     * then we move on and try to start something else...
     */
    fprintf(stderr, "shuffler: forward_reqs_now failed (%d)\n", rv);
    fprintf(stderr, "shuffler: DROPPED DATA!!  NOT GOOD!!\n");
    forw_start_next(oq, oput);
  }

  return(rv);
}

/*
 * forw_cb: normally the callback from an HG_Forward() operation
 * (runs in the context of the network thread via HG_Trigger()).
 * also directly called from forward_reqs_now() on an error.
 * our job is to mark this send as complete (dropping nsending) and
 * then see if there is anything on the wait queue that we can
 * advance (now that we just made space).
 *
 * @param cbi callback info (our arg, handle)
 * @return success
 */
static hg_return_t forw_cb(const struct hg_cb_info *cbi) {
  struct output *oput = (struct output *)cbi->arg;
  hg_handle_t hand;
  rpcout_t out;

  if (cbi->type != HG_CB_FORWARD) {
    fprintf(stderr, "cbi->type != FORWARD, impossible!\n");
    abort();
  }
  if (cbi->ret != HG_SUCCESS) {
    fprintf(stderr, "shuffle: forw_cb() failed (%d)\n", cbi->ret);
    fprintf(stderr, "shuffle: may have lost data!\n");
  }
  hand = cbi->info.forward.handle;
  
  if (hand && cbi->ret == HG_SUCCESS) {
    if (HG_Get_output(hand, &out) != HG_SUCCESS) {
      /* shouldn't ever happen, output is just 3 numbers */
      fprintf(stderr, "shuffler: forw_cb: get output failed\n");
    } else {
      if (out.ret != HG_SUCCESS) {
        fprintf(stderr, "shuffler: forw_cb: RPC %d failed (%d)\n",
          out.seq, out.ret);
      }
      HG_Free_output(hand, &out);
    }
  }

  /* destroy handle, drop nsending, and start next req */
  forw_start_next(oput->oqp, oput);

  return(HG_SUCCESS);
}

/*
 * forw_start_next: we have finished processing a handle (success
 * or failure) and need to destroy the handle, remove anything we 
 * sent from the queues, drop nsending, and then start anything on 
 * the waitq that can go.
 * 
 * @param oq the output queue we are working on
 * @param oput output we just sent (can be NULL if we had an error)
 */
static void forw_start_next(struct outqueue *oq, struct output *oput) {
  bool tosend, flush_done, flushloadingnow, empty_outs;
  struct request_queue tosendq;
  struct req_parent *fq, **fq_end, *parent, *nparent;
  struct request *req;

  /*
   * get rid of handle if we've got one (XXX should we try and recycle 
   * it?  how much memory does caching handles cost us?)
   */
  if (oput && oput->outhand) {
    HG_Destroy(oput->outhand);
    oput->outhand = NULL;
  }

  /* now lock the queue so we can drop nsending and advance */
  pthread_mutex_lock(&oq->oqlock);
  flush_done = false;
  if (oput) {

    /* flushing?  see if we finished everying at and before oqflush_output */
    if (oq->oqflushing && oput == oq->oqflush_output) {

      if (oput == XTAILQ_FIRST(&oq->outs)) {  /* nothing before us? */
        flush_done = true; /* so we call done_oq_flush() after unlock */
        oq->oqflushing = 0;
        oq->oqflush_output = NULL;
      } else {
        /* set oqflush_output to pending earlier request */
        oq->oqflush_output = XTAILQ_PREV(oput, sending_outputs, q);
      }

    }

    XTAILQ_REMOVE(&oq->outs, oput, q);
    free(oput);
    oput = NULL;
  }
  if (oq->nsending > 0) oq->nsending--;

  tosend = false;
  flushloadingnow = false;
  XSIMPLEQ_INIT(&tosendq);   /* to be safe */
  fq = NULL;
  fq_end = &fq;
  while (!oq->oqwaitq.empty() && tosend == false) {
    req = oq->oqwaitq.front();
    oq->oqwaitq.pop();

    /* if flushing, see if we pulled the last req of interest */
    if (oq->oqflushing && oq->oqflush_waitcounter > 0) {
      oq->oqflush_waitcounter--;
      if (oq->oqflush_waitcounter == 0) {
        flushloadingnow = true;   /* done first phase of flush */
      }
    }

    parent = req->owner;
    if (parent == NULL) {

      /* should never happen */
      fprintf(stderr, "shuffle: forw_cb: waitq req w/o owner?!?!\n");

    } else if (hg_atomic_decr32(&parent->nrefs) < 1) {   /* drop reference */

      if (parent->onfq) {               /* onfq is a sanity check */
        /* should never happen */
        fprintf(stderr, "shuffle_forw_cb: failed onfq sanity check!!!\n");
      } else {
        /* done with parent, put on a list for stopwait()... */
        *fq_end = parent;
        fq_end = &parent->fqnext;
        parent->onfq = 1;              /* now on an fq list */
      }

    }
     
    /* this bumps nsending back up if it returns a "tosend" list */
    tosend = append_req_to_locked_outqueue(oq->myset, oq, req, &tosendq, false);
  }

  /* if flushing, ensure our req got pushed out */
  if (flushloadingnow && !tosend) {
    tosend = append_req_to_locked_outqueue(oq->myset, oq, NULL, 
                                           &tosendq, true);
  }
  pthread_mutex_unlock(&oq->oqlock);

  /*
   * now we can stopwait() any parent whose nrefs dropped to zero.
   * (we've saved them all on the "fq" list so we could delay the
   * actual calls to stopwait() until after we've released the oqlock.)
   */
  for (parent = fq ; parent != NULL ; parent = nparent) {
    nparent = parent->fqnext;  /* save copy, we are going to free parent */
    parent_stopwait(oq->myset->shuf, parent, 0);   /* might HG_Respond, etc. */
  }

  /* if waitq gave us enough to start sending, do it now */
  if (tosend) {
    /* this will print an warning on failure */
    (void) forward_reqs_now(&tosendq, oq->myset->shuf, oq->myset, oq);

    /* if we are flushing and drained oqwaitq, start output tracking */
    if (flushloadingnow) {
      pthread_mutex_lock(&oq->oqlock);
      oq->oqflush_output = XTAILQ_LAST(&oq->outs, sending_outputs);
      empty_outs = (oq->oqflush_output == NULL);
      if (empty_outs) {     /* unlikely, but possible */
        oq->oqflushing = 0;
        flush_done = true;  /* trigger call to done_oq_flush, below */
      }
      pthread_mutex_unlock(&oq->oqlock);

    }
  }

  /* if we finished the flush, pass that info upward */
  if (flush_done) {
    done_oq_flush(oq);
  }

}

/*
 * shuffler_rpchand: mercury callback when we recv an RPC.  we need to
 * unpack the requests in the batch and use nexus to forward them on
 * to their next hop.  we'll allocate a req_parent to own any req that
 * gets placed on a waitq.  being placed on a waitq will cause our 
 * HG_Respond() to be delayed until everything clears the wait queue.

 *
 * @param handle the handle from the RPC request
 * @return success
 */
static hg_return_t shuffler_rpchand(hg_handle_t handle) {
  const struct hg_info *hgi;
  struct outset *inoset, *outoset;
  struct shuffler *sh; 
  int islocal, rank;
  hg_return_t ret;
  rpcin_t in;
  struct request *req;
  nexus_ret_t nexus;
  hg_addr_t dstaddr;
  struct req_parent *parent = NULL;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;
  rpcout_t reply;

  /* recover output queue set from handle and see if it is local or remote */
  hgi = HG_Get_info(handle);
  if (!hgi) {
    fprintf(stderr, "shuffler_rpchand: no hg_info (%p)\n", handle);
    abort();   /* should never happen */
  }
  inoset = (struct outset *) HG_Registered_data(hgi->hg_class, hgi->id);
  if (!inoset) {
    fprintf(stderr, "shuffler_rpchand: no registered data (%p)\n", handle);
    abort();   /* should never happen */
  }
  sh = inoset->shuf;
  islocal = (inoset == &sh->localq);

  /* if sending is disabled, we don't want new requests */
  if (sh->disablesend) {
    HG_Destroy(handle);
    return(HG_CANCELED);
  }

  /* decode RPC input into an rpcin_t */
  ret = HG_Get_input(handle, &in);
  if (ret != HG_SUCCESS) {
    fprintf(stderr, "shuffler_rpchand: HG_Get_input failed (%d)\n", ret);
    HG_Destroy(handle);
    return(ret);
  }

  /*
   * now we've got a list of reqs to either deliver local or forward
   * to their next hop...   if any requests get put on a wait queue,
   * then we need to allocate a req_parent to track the state of this
   * RPC so that we can delay the HG_Respond() until everything clears
   * the wait queue (this is for flow control).   we delay the allocation
   * of the req_parent until its first use (in case we don't need it).
   */
  while ((req = XSIMPLEQ_FIRST(&in.inreqs)) != NULL) {

    /* remove req from front of list */
    XSIMPLEQ_REMOVE_HEAD(&in.inreqs, next);

    /* determine next hop */
    nexus = nexus_next_hop(sh->nxp, req->dst, &rank, &dstaddr);

    /* case 1: we are dst of this request */
    if (nexus == NX_DONE) {

      ret = req_to_self(sh, req, handle, in.seq, &parent);

      continue;
    }

    /* case 2: not for us, sending request over mercury */

    /*
     * possible sub-cases for case 2:
     *   NX_ISLOCAL:  dst is on local node, use na+sm to output to final dst
     *                 -> input should be from network (we are DSTREP)
     *   NX_DSTREP:   dst is on remote node, use network to output
     *                 -> input should be from shm (we are SRCREP)
     *   NX_SRCREP:   dst is on remote node, need to forw to SRCREP
     *                 -> can't happen!  only happens at SRC.
     *                    all SRC routing happens in shuffler_send(),
     *                    never in shuffler_rpchand().
     *
     * sanity check it here to avoid network loops
     */
    if ((nexus != NX_ISLOCAL && nexus != NX_DESTREP) ||
        (nexus == NX_ISLOCAL && islocal)             ||
        (nexus == NX_DESTREP && !islocal)) {
      fprintf(stderr, "shuffler_rpchand: nexus panic!  code=%d, local=%d\n", 
              nexus, islocal);
      free(req);
      continue;
    }

    /* need to find correct output queue for dstaddr */
    outoset = (nexus == NX_DESTREP) ? &sh->remoteq : &sh->localq;
    it = outoset->oqs.find(dstaddr);
    if (it == outoset->oqs.end()) {
      /*
       * nexus knew the addr, but we couldn't find a a queue!
       * this should not happen!!!
       */
      fprintf(stderr, "shuffler_rpchand: no route for %d (%d)\n", req->dst,
              nexus);
      free(req);
      continue;
    }

    oq = it->second;    /* now we have the correct output queue */

    ret = req_via_mercury(sh, outoset, oq, req, handle, in.seq, &parent);

  }

  /*
   * if we malloc'd a req_parent via req_parent_init() [called in either
   * req_to_self or req_via_mercury], then we are holding an additional
   * reference to the parent to keep it in place until we exit the 
   * while loop above (req_parent_init set the inital value of nrefs to 2).
   * now we can drop that extra reference, since we are all done 
   * processing.
   * 
   * on the other hand, if we did not malloc a req_parent then the
   * RPC is done and we can respond right now.
   */
  if (parent != NULL) {
    (void) HG_Free_input(handle, &in);
    parent_dref_stopwait(sh, parent, 0);
  } else {
    reply.seq = in.seq;
    reply.from = sh->nxp->grank; /* XXX: layering */
    reply.ret = ret;
    (void) HG_Free_input(handle, &in);
    ret = HG_Respond(handle, shuffler_desthand_cb, handle, &reply);
    if (ret != HG_SUCCESS)
      HG_Destroy(handle);
  }

  return(HG_SUCCESS);
}

/*
 * shuffler_desthand_cb: sent reply, drop the handle
 *
 * @param cbi the arg for the callback
 * @return success
 */
static hg_return_t shuffler_desthand_cb(const struct hg_cb_info *cbi) {
  hg_handle_t handle = (hg_handle_t)cbi->arg;
  HG_Destroy(handle);
  return(HG_SUCCESS);
}

/*
 * aquire_flush: flush operations are serialized.  this function
 * blocks until a flush can run...  flush type is one of localq,
 * remoteq, or deliver.
 *
 * for localq/remoteq if we are successful we set oqflushing=1
 * and init the oqflush_counter to 1 (to hold it until the caller
 * can sleep).
 *
 * @param sh the shuffler we are using
 * @param fop the op that needs to flush, has not been init'd
 * @param type the type of flush we are planning to run
 * @param oset the output set (if type is localq or remoteq)
 * @return status (normally success after waiting)
 */
static hg_return_t aquire_flush(struct shuffler *sh, struct flush_op *fop,
                                int type, struct outset *oset) {
  hg_return_t rv = HG_SUCCESS;

  /* first init the flush operation's CV */
  if (pthread_cond_init(&fop->flush_waitcv, NULL) != 0) {
    fprintf(stderr, "shuffler: flush cv init failed!\n");
    return(HG_OTHER_ERROR);
  }

  pthread_mutex_lock(&sh->flushlock);
  fop->status = (sh->flushbusy) ? FLUSHQ_PENDING : FLUSHQ_READY;

  /* if flush is busy, our op needs to wait for it */
  if (fop->status == FLUSHQ_PENDING) {
    XSIMPLEQ_INSERT_TAIL(&sh->fpending, fop, fq);
    while (fop->status == FLUSHQ_PENDING) {
      pthread_cond_wait(&fop->flush_waitcv, &sh->flushlock);
    }

    /* wakeup removed us from pending queue, see if we were canceled */
    if (fop->status == FLUSHQ_CANCEL) {
      fprintf(stderr, "shuffler: aqflush: cancel while waiting\n");
      pthread_cond_destroy(&fop->flush_waitcv);
      return(HG_CANCELED);
    }
  }

  /* setup state for this flush */
  sh->flushbusy = 1;
  sh->curflush = fop;
  sh->flushdone = 0;
  sh->flushtype = type;
  sh->flushoset = oset;

  /* if we have an oset, then additional work todo while holding flushlock */
  if (oset) {
    oset->oqflushing = 1;
    hg_atomic_set32(&oset->oqflush_counter, 1);
  }

  pthread_mutex_unlock(&sh->flushlock);

  /* make sure we are still running or we might block forever... */
  if ((type == FLUSH_LOCALQ  && 
        (sh->localq.nshutdown  != 0 || sh->localq.nrunning  == 0)) ||
      (type == FLUSH_REMOTEQ && 
        (sh->remoteq.nshutdown != 0 || sh->remoteq.nrunning == 0)) || 
      (type == FLUSH_DELIVER && (sh->dshutdown != 0 || sh->drunning == 0)) ) {

    drop_curflush(sh);
    rv = HG_CANCELED;
  }
  
  return(rv);
}

/*
 * drop_curflush: we are the flusher, but we are done with it.  drop it
 * and wake up anyone waiting on the pending list to flush.
 *
 * @param sh the shuffler we are using
 */
static void drop_curflush(struct shuffler *sh) {
  struct flush_op *nxtfop;

  pthread_mutex_lock(&sh->flushlock);
  if (sh->flushbusy) {
    sh->flushbusy = 0;
    pthread_cond_destroy(&sh->curflush->flush_waitcv);
    sh->curflush = NULL;
    sh->flushtype = FLUSH_NONE;   /* to be safe */
    if (sh->flushoset) {
      sh->flushoset->oqflushing = 0;
      /* no need to set oqflush_counter */
    }
    nxtfop = XSIMPLEQ_FIRST(&sh->fpending);
    if (nxtfop != NULL) {
      XSIMPLEQ_REMOVE_HEAD(&sh->fpending, fq);
      nxtfop->status = FLUSHQ_READY;
      pthread_cond_signal(&nxtfop->flush_waitcv);
    }
  }
  pthread_mutex_unlock(&sh->flushlock);
}

/*
 * shuffler_flush_delivery: flush the delivery queue.  this function
 * blocks until all requests currently in the delivery queues (both
 * deliverq and dwaitq) are delivered.
 */
hg_return_t shuffler_flush_delivery(shuffler_t sh) {
  struct flush_op fop;
  hg_return_t rv;
  int waitcount;

  rv = aquire_flush(sh, &fop, FLUSH_DELIVER, NULL);    /* may BLOCK here */
  if (rv != HG_SUCCESS)
    return(rv);

  /* 
   * we now own the current flush operation, set counter and wait.
   * counter is dropped after we deliver a req with the callback
   * and will send us a cond_signal when it drops from 1 to zero.
   */
  pthread_mutex_lock(&sh->deliverlock);
  sh->dflush_counter = sh->deliverq.size() + sh->dwaitq.size();
  while (sh->dflush_counter > 0 && fop.status == FLUSHQ_READY) {
    pthread_cond_wait(&fop.flush_waitcv, &sh->deliverlock);  /* BLOCK HERE */
  }
  sh->dflush_counter = 0;
  pthread_mutex_unlock(&sh->deliverlock);
  
  drop_curflush(sh);

  rv = (fop.status == FLUSHQ_CANCEL) ? HG_CANCELED : HG_SUCCESS;
  return(rv);
}

/*
 * shuffler_flush_qs: flush either local or remote output queues.
 * this function blocks until all requests currently in the specified
 * output queues are delivered. We make no claims about requests that
 * arrive after the flush has been started.
 */
hg_return_t shuffler_flush_qs(shuffler_t sh, int islocal) {
  struct flush_op fop;
  hg_return_t rv;
  int type, r;
  struct outset *oset;
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;

  /* no point trying to flush if we can't send */
  if (sh->disablesend)
    return(HG_CANCELED);

  type = (islocal) ? FLUSH_LOCALQ : FLUSH_REMOTEQ;
  oset = (islocal) ? &sh->localq  : &sh->remoteq;

  rv = aquire_flush(sh, &fop, type, oset);         /* may BLOCK here */
  if (rv != HG_SUCCESS) {
    return(rv);
  }

  /* 
   * we've aquired the flush, including setting oqflushing and 
   * oqflush_counter both to 1.  (initing oqflush_counter to 1
   * rather than 0 keeps the flush active while we are setting 
   * it up -- we'll drop the extra reference before we block).  
   * now we need to look for output queues to be flushed. ...
   */
  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

    /* 
     * if we start a queue flush on this queue, bump the counter 
     * (keeping track of the number of outqueues being flushed).
     */
    if (start_qflush(sh, oset, oq)) {
      hg_atomic_incr32(&oset->oqflush_counter);
    }
  }

  /*
   * now wait for the flush to finish ... we use the flushlock here,
   * since the outset structure doesn't have a lock
   */
  pthread_mutex_lock(&sh->flushlock);
  r = hg_atomic_decr32(&oset->oqflush_counter);  /* drop our reference */
  if (r == 0)
    oset->oqflushing = 0;

  while (oset->oqflushing != 0 && fop.status == FLUSHQ_READY) {
    pthread_cond_wait(&fop.flush_waitcv, &sh->flushlock);  /* BLOCK HERE */
  }
  pthread_mutex_unlock(&sh->flushlock);

  /*
   * done!   drop the flush and return...
   */
  if (fop.status == FLUSHQ_CANCEL) {
    clean_qflush(sh, oset);    /* clear out state of cancel'd flush */
  }
  drop_curflush(sh);
  rv = (fop.status == FLUSHQ_CANCEL) ? HG_CANCELED : HG_SUCCESS;
  return(rv);
}

/*
 * start_qflush: start flushing an output queue.  if the queue flush
 * is pending, we return 1.  otherwise 0.   flushing an output queue 
 * is a multi-step process.  first we must wait for the waitq to drain.
 * second, if there are any pending buffered reqs in the loading list
 * waiting for enough data to build a batch then we need to stop waiting
 * and send them now.  third, we need to wait for all sending_outputs
 * on oq->outs at the time of the flush to finish.   depending on the
 * state of the queue we may be able to skip some or all of these 
 * steps (e.g. if queue empty, then we're done!).
 *
 * @param sh the shuffler we are using
 * @param oset the output set being flushed
 * @param oq the output queue to flush
 * @return 1 if flush is pending, otherwise zero
 */
static int start_qflush(struct shuffler *sh, struct outset *oset, 
                        struct outqueue *oq) {
  int rv = 0;
  bool tosend;
  struct request_queue tosendq;

  pthread_mutex_lock(&oq->oqlock);

  if (oq->oqflushing) {
    fprintf(stderr, "shuffler: start_qflush: oq->flushing already set?!\n");
    abort();    /* this shouldn't happen */
  }

  /* first, look for waiting requests in the oq->waitq */
  if (!oq->oqwaitq.empty()) {
    oq->oqflush_waitcounter = oq->oqwaitq.size();
    oq->oqflush_output = NULL;   /* to be safe */
    oq->oqflushing = 1;
    rv = 1;
    goto done;
  }

  /* second, flush the loading list (req==NULL in below call) */
  tosend = append_req_to_locked_outqueue(oset, oq, NULL, &tosendq, true);

  /* send?  drop oq lock to be safe since we are calling out to mercury */
  if (tosend) {
    pthread_mutex_unlock(&oq->oqlock);
    if (forward_reqs_now(&tosendq, sh, oset, oq) != HG_SUCCESS) {
      /* XXX: no good recovery from this */
      fprintf(stderr, "shuffler: start_qflush: forward_reqs_now failed?!\n");
    }
    pthread_mutex_lock(&oq->oqlock);
  }

  /* third, check oq->outs */
  if (XTAILQ_FIRST(&oq->outs) != NULL) {
    oq->oqflush_waitcounter = 0;
    oq->oqflush_output = XTAILQ_LAST(&oq->outs, sending_outputs);
    oq->oqflushing = 1;
    rv = 1;
  }

done:
  pthread_mutex_unlock(&oq->oqlock);
  return(rv);
}

/*
 * clean_qflush: clean out state of a flush that has been canceled.
 * we own the flush so it is safe to clear out oset once all the 
 * output queues have been reset...
 *
 * @param sh the shuffler we are using
 * @param oset the output set being flushed
 * @return 1 if flush is pending, otherwise zero
 */
static void clean_qflush(struct shuffler *sh, struct outset *oset) {
  std::map<hg_addr_t, struct outqueue *>::iterator it;
  struct outqueue *oq;

  for (it = oset->oqs.begin() ; it != oset->oqs.end() ; it++) {
    oq = it->second;

    pthread_mutex_lock(&oq->oqlock);
    oq->oqflushing = 0;
    oq->oqflush_waitcounter = 0;
    oq->oqflush_output = NULL;
    pthread_mutex_unlock(&oq->oqlock);
  }

  oset->oqflushing = 0;
  hg_atomic_set32(&oset->oqflush_counter, 0);
}

/*
 * done_oq_flush: finished flushing an outqueue.  need to update the
 * outset and signal flusher if we dropped the ref to zero!
 *
 * @param oq the output queue we just finished flushing
 */
static void done_oq_flush(struct outqueue *oq) {
  struct outset *oset = oq->myset;
  struct shuffler *sh = oset->shuf;
  int r;

  r = hg_atomic_decr32(&oset->oqflush_counter);

  /* signal main flusher if we dropped the last reference */
  if (r == 0) {
    pthread_mutex_lock(&sh->flushlock);  /* protects oqflushing */
    oset->oqflushing = 0;
    pthread_cond_broadcast(&sh->curflush->flush_waitcv);
    pthread_mutex_unlock(&sh->flushlock);
  }
}

/*
 * shuffler_shutdown: stop all threads, release all memory.
 * does not shutdown mercury (since we didn't start it, nexus did),
 * but mercury should not be restarted once we call this.
 */
hg_return_t shuffler_shutdown(shuffler_t sh) {
  int cnt;

  /* stop all new inbound requests */
  sh->disablesend = 1;

  /* cancel any flush ops that are queued or running */
  shuffler_flush_discard(sh);

  /* stop all threads */
  stop_threads(sh);

  /* purge any orphaned reqs */
  cnt = purge_reqs(sh);
  if (cnt) {
    fprintf(stderr, "shuffler: shutdown warning: %d orphans\n", cnt);
  }

  /* now free remaining structure */
  shuffler_outset_discard(&sh->localq);     /* ensures maps are empty */
  shuffler_outset_discard(&sh->remoteq);  
  if (sh->funname) free(sh->funname);
  pthread_mutex_destroy(&sh->deliverlock);
  pthread_cond_destroy(&sh->delivercv);
  pthread_mutex_destroy(&sh->flushlock);
  delete sh;

  return(HG_SUCCESS);
}
