#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"
DATA_SCALE=$DIR/data_scale

gen_data(){
    SCL=$1

    if [[ ! -f ${DATA_SCALE} ]]; then
        touch ${DATA_SCALE}
    fi

    if [[ `cat ${DATA_SCALE}` == "$SCL" && -d ${DIR}/data ]]; then
        echo "Data exist"
    else
        clean_data
        cd $DIR/tpch-spark/dbgen
        ./dbgen -f -s ${SCL}

        cd $DIR

        mkdir -p data
        mv tpch-spark/dbgen/*.tbl data/

        echo "${SCL}" > ${DATA_SCALE}
    fi
}

move_data(){
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data

    for f in $(ls $DIR/data); do
        $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/$f $DIR/data/$f
    done

#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl $DIR/data/lineitem.tbl
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl $DIR/data/orders.tbl
}

move_data_hdfs(){
    $DIR/hadoop/bin/hadoop fs mkdir -p $DIR/data
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data

    for f in $(ls $DIR/data); do
        $DIR/hadoop/bin/hadoop fs -copyFromLocal $DIR/data/$f $DIR/data/$f
    done
}

save_par_data(){
    mkdir -p $DIR/tpch_parquet
    $DIR/alluxio/bin/alluxio fs copyToLocal $DIR/tpch_parquet $DIR/tpch_parquet
}

move_par_data(){
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/tpch_parquet

    for f in $(ls $DIR/tpch_parquet); do
        $DIR/alluxio/bin/alluxio fs mkdir $DIR/tpch_parquet/$f
        for sf in $(ls $DIR/tpch_parquet/$f); do
            $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/tpch_parquet/$f/$sf $DIR/tpch_parquet/$f/$sf
        done
    done
}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi

    if [[ -d tpch_parquet ]]; then
        rm -r tpch_parquet/
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

    sed -i "/alluxio.user.file.replication.min=2/c\alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
}

nonshuffle_env(){
    sed -i \
        "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
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