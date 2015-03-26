#!/usr/bin/env bash

# The following gives an example:

#the workspace dir in Tachyon
export TACHYON_PERF_WORKSPACE="/tmp/tachyon-perf-workspace"

#the report output path
export TACHYON_PERF_OUT_DIR="$TACHYON_PERF_HOME/result"

#the tachyon-perf master service address
TACHYON_PERF_MASTER_HOSTNAME="master"
TACHYON_PERF_MASTER_PORT=23333

#the threads num
TACHYON_PERF_THREADS_NUM=1

#the slave is considered to be failed if not register in this time
TACHYON_PERF_UNREGISTER_TIMEOUT_MS=10000

#if true, the TachyonPerfSupervision will print the names of those running and remaining nodes
TACHYON_PERF_STATUS_DEBUG="false"

#if true, the test will abort when the number of failed nodes more than the threshold
TACHYON_PERF_FAILED_ABORT="true"
TACHYON_PERF_FAILED_PERCENTAGE=1

# Choose ufs impl
TACHYON_PERF_UFS="THCI"

PERF_CONF_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export TACHYON_PERF_JAVA_OPTS+="
  -Dlog4j.configuration=file:$PERF_CONF_DIR/log4j.properties
  -Dtachyon.perf.failed.abort=$TACHYON_PERF_FAILED_ABORT
  -Dtachyon.perf.failed.percentage=$TACHYON_PERF_FAILED_PERCENTAGE
  -Dtachyon.perf.status.debug=$TACHYON_PERF_STATUS_DEBUG
  -Dtachyon.perf.master.hostname=$TACHYON_PERF_MASTER_HOSTNAME
  -Dtachyon.perf.master.port=$TACHYON_PERF_MASTER_PORT
  -Dtachyon.perf.work.dir=$TACHYON_PERF_WORKSPACE
  -Dtachyon.perf.out.dir=$TACHYON_PERF_OUT_DIR
  -Dtachyon.perf.threads.num=$TACHYON_PERF_THREADS_NUM
  -Dtachyon.perf.unregister.timeout.ms=$TACHYON_PERF_UNREGISTER_TIMEOUT_MS
  -Dtachyon.perf.ufs=$TACHYON_PERF_UFS
"
