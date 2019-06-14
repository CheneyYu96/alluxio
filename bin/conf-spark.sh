#!/usr/bin/env bash
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

add_name(){
#    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
#
#    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
#    worker_num=$(($worker_num-2))
#
#    for i in `seq 0 ${worker_num}`; do
#        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "echo 'export SPARK_LOCAL_HOSTNAME=\"${workers[$i]}\"' >>  /home/ec2-user/spark/conf/spark-env.sh"
#    done

    workers=(`hdfs dfsadmin -printTopology | grep 172`)
    worker_num=(`hdfs dfsadmin -printTopology | grep 172 | wc -l`)
    worker_num=$(($worker_num-1))

    for i in `seq 0 ${worker_num}`; do
        w_ip=(`echo ${workers[$i]} | cut -d ':' -f 1`)
        w_ip=${w_ip:3}

        echo ${w_ip}

        name=(`echo ${workers[$i]} | cut -d '(' -f 2 | cut -d ')' -f 1`)
        echo ${name}
#        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "echo 'export SPARK_LOCAL_HOSTNAME=\"${workers[$i]}\"' >>  /home/ec2-user/spark/conf/spark-env.sh"
    done
}

remove_last(){
    cd ${LOCAL_DIR}
    ./alluxio copyDir ../../spark/conf
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