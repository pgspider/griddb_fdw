#!/bin/bash
# script restart griddb server

GRIDDB_HOME=${HOME}/src/griddb-5.1.0

export GS_HOME=${GRIDDB_HOME}
export GS_LOG=${GRIDDB_HOME}/log

# stop griddb server
${GRIDDB_HOME}/bin/gs_leavecluster -w -f -u admin/testadmin
${GRIDDB_HOME}/bin/gs_stopnode -w -u admin/testadmin

sleep 10

# start griddb server
${GRIDDB_HOME}/bin/gs_startnode -w -u admin/testadmin
${GRIDDB_HOME}/bin/gs_joincluster -w -c griddbfdwTestCluster -u admin/testadmin
