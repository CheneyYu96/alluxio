#!/usr/bin/env bash


LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

PREFIX=$DIR/spark/jars
BACKUP=$DIR/spark/backup
mkdir -p $BACKUP

for f in $(ls $DIR/alluxio/jars); do
    mv ${PREFIX}/${f} ${BACKUP}
    cp ${DIR}/alluxio/jars/${f} ${PREFIX}
done