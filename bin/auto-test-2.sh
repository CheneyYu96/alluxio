#!/usr/bin/env bash
set -euxo pipefail
#set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

pre_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL -T O
    ./dbgen -s $SCL -T L

    cd $DIR
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    alluxio/bin/alluxio fs rm -R /tpch
}

# include shuffle & non shuffle
all() {
    SCALE=$1
    CORES=$2 # for further use

    total_cores=$[$CORES*2]

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    #shuffle
    $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${CORES} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads0.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads1.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    # spread data
    for ((i=1;i<=2;i++)); do
        $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${CORES} \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --app "Join warmup${i} scale${SCALE}" > $DIR/logs/noshuffle/warmup_${i}.log 2>&1
    done


    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    # formal experiment
    $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${CORES} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --app "Join nonshuffle scale${SCALE}" > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads0.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads1.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
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

auto_test() {
    bandwidth=$1
    cores=$2

    export SPARK_WORKER_CORES=${cores}
    upper_dir=/home/ec2-user/logs/cpu${cores}_bandwidth${bandwidth}
    mkdir -p ${upper_dir}

    for((i=1;i<=10;i++)); do
        scl=$i

        lower_dir=${upper_dir}/scale${scl}
        mkdir -p ${lower_dir}

        ${DIR}/alluxio/bin/restart.sh

        pre_data $scl
        limit_bandwidth ${bandwidth}
        test_bandwidth ${lower_dir}

        all ${scl} ${cores}
        mv $DIR/logs/noshuffle ${lower_dir}
        mv $DIR/logs/shuffle ${lower_dir}

        clean_data
        free_limit

    done

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

usage() {
    echo "Usage: $0 shffl|noshffl scale #query"
}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        all)                    all $2 $3
                                ;;
        pre)                    pre_data $2
                                ;;
        clean)                  clean_data
                                ;;
        free)                   free_limit
                                ;;
        limit)                  limit_bandwidth $2
                                ;;
        auto)                   auto_test $2 $3
                                ;;
        * )                     usage
    esac
fi