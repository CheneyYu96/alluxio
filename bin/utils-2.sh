#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

gen_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL

    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/

}

move_data(){
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data

    for f in $(ls $DIR/data); do
        $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/$f $DIR/data/$f
    done

#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl $DIR/data/lineitem.tbl
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl $DIR/data/orders.tbl
}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
}

free_limit(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; echo test"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; echo test"
}

limit_bandwidth(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    limit=$1
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $limit -u $limit"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $limit -u $limit"
}

test_bandwidth() {
    saved_dir=$1

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`);

    echo "Setup iperf3"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "iperf3 -s > /dev/null 2>&1 &"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "iperf3 -c ${workers[0]} -d" >> ${saved_dir}/bandwith.txt

    echo "kill iperf3 server process"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "pkill iperf3"
}

collect_workerloads(){
    worker_log_dir=$1
    name=$2

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${worker_log_dir}/workerLoads0_${name}.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${worker_log_dir}/workerLoads1_${name}.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
}

clear_workerloads(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
}

shuffle_env(){
    sed -i \
        "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
        $DIR/alluxio/conf/alluxio-site.properties
    sed -i \
        "/alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.LocalFirstPolicy/c\alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
        $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=2/c\alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
}

nonshuffle_env(){
    sed -i \
        "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
        $DIR/alluxio/conf/alluxio-site.properties
    sed -i \
        "/alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.LocalFirstPolicy" \
        $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=0/c\alluxio.user.file.replication.min=2" $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
}

get_dir_index(){
    NAME=$1

    INDEX=1
    while [[ -d $DIR/logs/${NAME}${INDEX} ]]
    do
        let INDEX++
    done

    echo $DIR/logs/${NAME}${INDEX}

}

if [[ "$#" -lt 2 ]]; then
    exit 1
else
    case $1 in
        gen_data)               gen_data $2
                                ;;
        move_data)              move_data
                                ;;
        clean_data)             clean_data
                                ;;
        free_limit)             free_limit
                                ;;
        limit_bandwidth)        limit_bandwidth $2
                                ;;
        test_bandwidth)         test_bandwidth $2
                                ;;
        collect_workerloads)    collect_workerloads
                                ;;
        shuffle_env)            shuffle_env
                                ;;
        nonshuffle_env)         nonshuffle_env
                                ;;
        get_dir_index)          get_dir_index $2
                                ;;
    esac
fi