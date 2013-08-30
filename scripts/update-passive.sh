#!/bin/bash

set -x

export GOMAXPROCS=4
export LD_LIBRARY_PATH=~/leveldb

EXE=$HOME/go/bin/bismark-passive-server-go
WORKERS=8
LEVELDB_ROOT=/data/users/sburnett/passive-leveldb-new
TARS_PATH=/data/users/sburnett/passive-organized

BASE_CMD="$EXE --workers=$WORKERS"
COMMON_FLAGS="--passive_leveldb_root=$LEVELDB_ROOT"

$BASE_CMD index $COMMON_FLAGS --tarballs_path=$TARS_PATH
$BASE_CMD availability $COMMON_FLAGS --json_output=$HOME/public_html/bismark-passive/status.json

export PGHOST=localhost
export PGPORT=54321
export PGDATABASE=ucap_deploy_db
export PGUSER=hyojoon
export PGPASSWORD=Databasejoon82

$BASE_CMD bytesperminute $COMMON_FLAGS
$BASE_CMD bytesperdevice $COMMON_FLAGS
$BASE_CMD bytesperdomain $COMMON_FLAGS

unset PGOST PGPORT PGDATABASE PGUSER PGPASSWORD
