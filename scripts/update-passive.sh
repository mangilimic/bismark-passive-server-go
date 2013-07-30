#!/bin/bash

set -x

export GOMAXPROCS=4
export LD_LIBRARY_PATH=~/leveldb

PKG_ROOT=$HOME/go/src/github.com/sburnett/bismark-passive-server-go/passive
PIPELINE_ARGS="--workers=8"
TARS_PATH=/data/users/sburnett/passive-organized
LEVELDB_PATH=/data/users/sburnett/passive-leveldb-new
OUTPUT_PATH=$HOME/bismark-health

PIPELINES_BIN=$PKG_ROOT/pipelines/pipelines

$PKG_ROOT/scanner/scanner $TARS_PATH $LEVELDB_PATH
$PIPELINES_BIN $PIPELINE_ARGS index $LEVELDB_PATH
$PIPELINES_BIN $PIPELINE_ARGS availability $LEVELDB_PATH --json_output=$HOME/public_html/bismark-passive/status.json

export PGHOST=localhost
export PGPORT=54321
export PGDATABASE=ucap_deploy_db
export PGUSER=hyojoon
export PGPASSWORD=Databasejoon82

$PIPELINES_BIN $PIPELINE_ARGS bytesperminute $LEVELDB_PATH
$PIPELINES_BIN $PIPELINE_ARGS bytesperdevice $LEVELDB_PATH
$PIPELINES_BIN $PIPELINE_ARGS bytesperdomain $LEVELDB_PATH
