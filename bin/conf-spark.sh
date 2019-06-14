#!/usr/bin/env bash
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
worker_num=$(($worker_num-2))

for i in `seq 0 ${worker_num}`; do
    ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "echo 'export SPARK_LOCAL_HOSTNAME=${workers[$i]}' >>  /home/ec2-user/spark/conf/spark-env.sh"
done