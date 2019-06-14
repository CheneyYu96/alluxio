#!/usr/bin/env bash
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

add_name(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    worker_num=$(($worker_num-2))

    for i in `seq 0 ${worker_num}`; do
        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "echo 'export SPARK_LOCAL_HOSTNAME=\"${workers[$i]}\"' >>  /home/ec2-user/spark/conf/spark-env.sh"
    done
}

remove_last(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    worker_num=$(($worker_num-2))

    for i in `seq 0 ${worker_num}`; do
        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "sed -i '\$d' /home/ec2-user/alluxio/conf/alluxio-site.properties"
    done
}

usage(){
    echo "require params"
}

if [[ "$#" -lt 1 ]]; then
    usage
    exit 1
else
    case $1 in
        add)                    add_name
                                ;;
        remove)                 remove_last
                                ;;
        * )                     usage
    esac
fi