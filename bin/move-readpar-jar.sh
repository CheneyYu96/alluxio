#!/usr/bin/env bash
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
worker_num=$(($worker_num-2))

for i in `seq 0 ${worker_num}`; do
    scp -o StrictHostKeyChecking=no /home/ec2-user/alluxio/readparquet/target/readparquet-2.0.0-SNAPSHOT.jar ec2-user@${workers[$i]}:/home/ec2-user/alluxio/readparquet/target/
done