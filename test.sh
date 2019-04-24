#!/bin/bash

#GRIDDB_HOME=${HOME}/src/griddb_nosql_4.0.0

if [[ ! -d "${GRIDDB_HOME}" ]]; then
  echo "GRIDDB_HOME environment variable not set"
  exit 1
fi

# Start GridDB server
export GS_HOME=${GRIDDB_HOME}
export GS_LOG=${GRIDDB_HOME}/log
if pgrep -x "gsserver" > /dev/null
then
  ${GRIDDB_HOME}/bin/gs_leavecluster -w -f -u admin/testadmin
  ${GRIDDB_HOME}/bin/gs_stopnode -w -u admin/testadmin
fi
rm -rf ${GS_HOME}/data/* ${GS_LOG}/*
sleep 1
echo "Starting GridDB server..."
${GRIDDB_HOME}/bin/gs_startnode -w -u admin/testadmin
${GRIDDB_HOME}/bin/gs_joincluster -w -c ktymCluster -u admin/testadmin

result="$?"
if [[ "$result" -eq 0 ]]; then
  #sed -i 's/REGRESS =.*/REGRESS = griddb_fdw_post/' Makefile
  make clean && make && make check
fi
