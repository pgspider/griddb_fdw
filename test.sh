#!/bin/bash

#GRIDDB_HOME=${HOME}/src/griddb_nosql_4.0.0

if [[ ! -d "${GRIDDB_HOME}" ]]; then
  echo "GRIDDB_HOME environment variable not set"
  exit 1
fi

rm -rf /tmp/*.data
find ./sql/ -name "*.data" -exec cp {} /tmp/ \;

# Start GridDB server
export GS_HOME=${GRIDDB_HOME}
export GS_LOG=${GRIDDB_HOME}/log
export no_proxy=127.0.0.1
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
  sed -i 's/REGRESS =.*/REGRESS = float4 float8 int4 int8 numeric join limit aggregates prepare select_having select insert update griddb_fdw_post/' Makefile
  gcc make_check_initializer/griddb_init_post.c -o griddb_init -Igriddb/client/c/include -Lgriddb/bin -lgridstore
  ./griddb_init 239.0.0.1 31999 ktymCluster admin testadmin
  gcc -I./griddb/client/c/include -L./griddb/bin make_check_initializer/griddb_init_core.c -lgridstore
  ./a.out 239.0.0.1 31999 ktymCluster admin testadmin
  make clean
  make
  make check | tee make_check.out
fi
