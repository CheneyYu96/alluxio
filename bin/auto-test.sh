#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
echo "worker0: ${workers[0]}"
echo "worker1: ${workers[1]}"

pre_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL -T O
    ./dbgen -s $SCL -T L

    cd $DIR
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/
}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    alluxio/bin/alluxio fs rm -R /tpch
}

download_data(){
    item=$1

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${item}/workerLoads0.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${item}/workerLoads1.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
}

all() {
    SCALE=$1
    QUERY=$2 # for further use

    mkdir -p  $DIR/logs/shuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    # shuffle
    echo "----------TESTING SHUFFLE----------"
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

    download_data shuffle

    # noshuffle
    echo "----------TESTING NO_SHUFFLE----------"
    # spread data
    for ((i=1;i<=2;i++)); do
        $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join warmup${i} scale${SCALE}" > $DIR/logs/noshuffle/warmup_${i}.log 2>&1
    done

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    # formal experiment
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join nonshuffle scale${SCALE}" > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1

    download_data noshuffle
}

usage() {
    echo "Usage: "
    echo "$0 all SCALE QUERY"
    echo "$0 pre SCALE"
    echo "$0 clean"
}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        all)                    all $2 $3
                                ;;
        pre)                    pre $2
                                ;;
        clean)                  clean_data
                                ;;
        * )                     usage
    esac
fi