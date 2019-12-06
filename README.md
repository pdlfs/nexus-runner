# nexus-runner

The nexus-runner program is an MPI-based test program for deltafs-nexus
and deltafs-shuffle (the 3 hop shuffle).  In a shuffle, we distribute
and send data across a set of processes using a destination rank number.

## 3 hop shuffle overview

The shuffle uses mercury RPCs to send a message from a SRC process
to a DST process in multiple hops.  The goal is to reduce per-process
memory usage by reducing the number of output queues and connections
to manage on each node (we are assuming an environment where each node
has multiple CPUs and there is an app with processes running on each
CPU).

```
 SRC  ---na+sm--->   SRCREP ---network--->   DSTREP   ---na+sm--->  DST
           1                      2                        3

note: "na+sm" is mercury's shared memory transport, "REP" == representative
```

We divide the job for sending to each remote node among the local
cores.  Furthermore, we only send to one process on each remote node.
We expect the remote receiving process to forward our message to
the final destination (if it isn't the final destination).  See
the deltafs-shuffle documentation for details.

## nexus-runner program

The nexus-runner program should be launched across a set of
one or more nodes using MPI (e.g. with mpirun).  Each process will
init the nexus routing library and the shuffle code.  Processes
will then send the requested number of messages through the 3 hop
shuffle, flush the system, and then exit.

The topology of the run is specified using MPI options.
The two variables are the number of nodes to use, and the number
of processes to run on each node.  The configuration of the
3 hop shuffle is specified using nexus-runner command line
flags.

## command line usage

```
usage: nexus-runner [options] mercury-protocol [subnet]

options:
        -c count    number of shuffle send ops to perform
        -e          exclude sending to self (skip sends)
        -f rate     do a flush every 'rate' sends
        -l          loop through dsts (no random sends)
        -N filespec nexus dump to this filespec
        -n minsndr  rank must be >= minsndr to send requests
        -o m        add 'm' msec output delay to delivery
        -p port     base port number
        -q          quiet mode
        -R rank     only do sends to this rank
        -r n        enable tag suffix with this run number
        -s maxsndr  only ranks <= maxsndr send requests
        -T          extra time/usage stats for instance
        -t sec      timeout (alarm), in seconds
        -x          use network hg prog for local reqs

shuffle queue config:
        -B bytes    batch buf target for network
        -a bytes    batch buf target for client/origin shm
        -b bytes    batch buf target for relayed shm
        -d count    delivery queue size limit
        -h count    delivery thread wakeup threshold
        -M count    maxrpcs for network output queues
        -m count    maxrpcs for shm client/origin queues
        -y count    maxrpcs for shm relayed queues
        -Z count    remote RPC limit on shuffle_enqueue
        -z count    local RPC limit on shuffle_enqueue

size related options:
        -i size     input req size (>= 24 if specified)

default payload size is 24.

logging related options (rank <= max can have xtra logging, use -X):
        -C mask      mask cfg for non-extra rank procs
        -E mask      mask cfg for extra rank procs
        -D priority  default log priority
        -F logfile   logfile (rank # will be appended)
        -I n         message buffer size (0=disable)
        -L           enable logging
        -O options   opts (a=alllogs,s=stderr,x=xtra stderr)
        -S priority  print to stderr priority
        -X n         max extra rank#

```

The program prints the current set of options at startup time.
The default count is 5 send ops, and the default timeout is 120
seconds.  The timeout is to prevent the program from hanging forever if
there is a problem with the transport.   Quiet mode can be used
to prevent the program from printing during the RPCs (so that printing
does not impact performance).

By default, destinations are chosen randomly, but if "-l" is used
the program will instead loop through the list of nodes.  "-s" can
be used to limit the number of processes sending requests.  For example,
"-s 0" means that only rank 0 sends requests (the rest just receive
them).

The shuffle queue configuration flags are used to configure the 3
hop shuffle.  There are limits for local shared memory and network
communications.  The batch buf target is the number of output bytes the
shuffle caches before sending out a batch of requests in an RPC.
If the target is 1, then batching is disabled.  If the target is
1MB, then the system will send an RPC once it has received a total
of 1MB of shuffle send requests.  The maxrpcs limits the number of
outstanding mercury RPC operations allowed for a destination.  If
this is 1, then that disables having multiple RPCs pending for a
given output host.  The delivery queue size limit sets the limit
for the number of pending request delivery ops the shuffle will
queue in memory before applying flow control to the delivery process.

The shuffle has extensive logging that can be enabled by setting
the "-L" flag.  The default logging priorities are set by the "-D"
and "-S" flags.  Priority levels are based on syslog(3) and include:
EMERG, ALERT, CRIT, ERR, WARN, NOTE, INFO, and DEBUG.  Debug
messages are divided into 4 streams at the same priority level
(D0, D1, D2, and D3).  Each stream can be selected as desired (or
all debug msgs can be selected with "DEBUG").  Messages are logged
if they at or above the current priority.  Logged messages are also
printed to stderr if they are at or above the stderr priority or
one of the stderr "-O" options is specified.   Log messages can
be saved in memory in a circular message buffer (specify the size
using "-I"), and/or saved in a log file (use "-F" to specify the
filename -- note that the rank of the process will be appended to
the filename).  Note that log data in the memory buffer will get
saved to a core file in the event of a crash (in case you want
to examine the log at the time of the crash).

For the purpose of logging, the processes are divided into two
groups using the "-X rank" flag.  Processes whose rank is less
than or equal to the "-X" value are targeted for extra logging.
Log files ("-F") are only created for the extra logging processes
unless the "-O a" option is specified.  The logging masks for
the processes can be tuned with "-C" and "-E" ... these flags
allow a log mask to be specified by facility using a format like:

SHUF=INFO,UTIL=ERR,CLNT=DEBUG

The current list of facilities available is: SHUF (general messages),
UTIL (utility functions), CLNT (client APIs functions), and DLVR
(delivery thread).

## examples

Run 4 processes on one node.  Only proc 0 sends messages, and it
sends one message to each destination:

```
mpirun -n 4 nexus-runner -c 4 -l -s 0 bmi+tcp 10

```

Above, with full logging (in "/tmp/log.[0-3]"):
```
mpirun -n 4 build/nexus-runner -c 4 -l -s 0 \
	-L -C DEBUG -E DEBUG -O a -F /tmp/log bmi+tcp 10

```

## to compile

First, you need to know where deltafs-shuffle, deltafs-nexus and mercury
are installed.  You also need cmake.  To compile with a build subdirectory,
starting from the top-level source dir:

```
  mkdir build
  cd build
  cmake -DCMAKE_PREFIX_PATH=/path/to/installs ..
  make
```

That will produce binaries in the current directory.  "make install"
will install the binaries in CMAKE_INSTALL_PREFIX/bin (defaults to
/usr/local/bin) ... add a -DCMAKE_INSTALL_PREFIX=dir to change the
install prefix from /usr/local to something else.
