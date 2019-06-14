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

    workers=(`hdfs dfsadmin -printTopology | grep ip`)
    worker_num=(`hdfs dfsadmin -printTopology | grep ip | wc -l`)
    worker_num=$(($worker_num-1))

    for i in `seq 0 ${worker_num}`; do
        w_ip=(`echo ${workers[$((i*2))]} | cut -d ':' -f 1`)

        echo ${w_ip}

        name=(`echo ${workers[$((i*2+1))]} | cut -d '(' -f 2 | cut -d ')' -f 1`)
        echo ${name}
#        ssh ec2-user@${w_ip} -o StrictHostKeyChecking=no "echo 'export SPARK_LOCAL_HOSTNAME=\"${w_ip}\"' >>  /home/ec2-user/spark/conf/spark-env.sh"

        master_url=$(cat /home/ec2-user/hadoop/conf/masters)
        ssh ec2-user@${w_ip} -o StrictHostKeyChecking=no "/home/ec2-user/spark/sbin/start-slave.sh -h ${w_ip} spark://${master_url}:7077"
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