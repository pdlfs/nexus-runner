# NEEDS TO BE UPDATED FOR nexus-runner

# mercury-runner

The mercury-runner program is a multi-instance mercury send/recv
test program.  It contains both a mercury RPC client and RPC server.
The client sends "count" number of RPC requests and exits when
all replies are in.  The server receives "count" number of RPC
  requests and exits when all requests have been processed.

To use the program you need to run two copies of it.  By
default both client and server are active so RPCs flow
in both directions (so each process is a peer).   If you only
want a one way flow of RPC calls, run one copy of the program
as a client and run the other as a server.

The mercury-runner program can run multiple instances of mercury
in the same process.   In this case, server port numbers are assigned
sequentially starting at the "baseport" (default value 19900).
On the command line, specify the port numbers as a printf "%d"
and the program will fill in the value.  For client-only mode,
we init the client side with port numbers that are just past
the server port numbers.

Note: the program currently requires that the number of instances
and number of RPC requests to create and send to match between the
processes.

By default the client side of the program sends as many RPCs as
possible in parallel.  You can limit the number of active RPCs
using the "-l" flag.  Specifying "-l 1" will cause the client side
of the program to fully serialize all RPC calls.

# command line usage

```
usage: ../bin/mercury-runner [options] ninstances localspec [remotespec]


local and remote spec are mercury urls.
use printf '%d' for the port number.
remotespec is optional if mode is set to 's' (server)

options:
	-c count    number of RPCs to perform
	-d dir      shared dir for passing server addresses
	-l limit    limit # of client concurrent RPCs
	-m mode     mode c, s, or cs (client/server)
	-p port     base port number
	-q          quiet mode
	-r n        enable tag suffix with this run number
	-t sec      timeout (alarm), in seconds

use '-l 1' to serialize RPCs

size related options:
	-i size     input req size (>= 8 if specified)
	-o size     output req size (>= 8 if specified)
	-L size     server's local rma buffer size
	-S size     client bulk send sz (srvr RMA reads)
	-R size     client bulk recv sz (srvr RMA writes)
	-O          one buffer flag (valid if -S and -R set)
	-X count    client call handle cache max size
	-Y count    server reply handle cache max size

default payload size is 4.
to enable RMA:
  must specify -L (on srvr) and -S and/or -R (on cli)
using -O causes the server to RMA read and write to the
same buffer (client exports it in RDWR mode)
default value for -L is 0 (disables RMA on server)
for -X/-Y: count=-1 disable cache, count=0 unlimited
```

The program prints the current set of options at startup time.
The default count is 5 RPCs, the default limit is set to the count
(the largest possible value), and the default timeout is 120 seconds.
The timeout is to prevent the program from hanging forever if
there is a problem with the transport.   Quiet mode can be used
to prevent the program from printing during the RPCs (so that printing
does not impact performance).

The -L/-S/-R options use the Mercury HG_Bulk_transfer bulk transfer
operation to move data.

# examples

 one client and one server mode, serialized sending, one instance:

```
      client:
      ./mercury-runner -l 1 -c 50 -q -m c 1 cci+tcp://10.93.1.210:%d \
                             cci+tcp://10.93.1.233:%d
      server:
      ./mercury-runner -c 50 -q -m s 1 cci+tcp://10.93.1.233:%d
```
Note that the -c's must match on both sides.

both processes send and recv RPCs (client and server), one
instance, both sides sending in parallel:

```
      ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.210:%d \
                             cci+tcp://10.93.1.233:%d

      ./mercury-runner -c 50 -q -m cs 1 cci+tcp://10.93.1.233:%d \
                             cci+tcp://10.93.1.210:%d
```

Check the Mercury documentation to see what transports are supported
(cci, bmi, gni, etc.).

The -d option can be used to pass addresses between processes via
a shared directory.  This is required in order to use transports
such as MPI.   When using -d, the server address is given in the
form tag=transport (e.g. "h0=bmi+tcp").  The transport text is
used to init Mercury.  The program will then queue Mercury for
the local address used to establish the server and write the address
in a file in the shared directory with the "tag" string in it.

To connect to a client, specify a shared -d directory and the tag.
The program will then look in the shared directory for the address
file with the given tag.

Here's an example of using -d with the current directory (as printed
by the 'pwd' command).  The first command starts a server under the
tag "h0" and attempts to connect to a server under a tag "h1" (the
second command does the opposite).
```
      ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs 1 h0=bmi+tcp h1
      ./mercury-runner -l 16 -d `pwd` -q -c 1000 -m cs 1 h1=bmi+tcp h0
```

# to compile

First, you need to know where mercury is installed and you need cmake.
To compile with a build subdirectory, starting from the top-level
source dir:

```
  mkdir build
  cd build
  cmake -DCMAKE_PREFIX_PATH=/path/to/mercury-install ..
  make
```

That will produce binaries in the current directory.  "make install"
will install the binaries in CMAKE_INSTALL_PREFIX/bin (defaults to
/usr/local/bin) ... add a -DCMAKE_INSTALL_PREFIX=dir to change the
install prefix from /usr/local to something else.
