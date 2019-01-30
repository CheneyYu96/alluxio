#!/usr/bin/env bash
set -euxo pipefail
set -x

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
    QUERY=$2 # for further use

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    #shuffle
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log

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
        $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join warmup${i} scale${SCALE}" > $DIR/logs/noshuffle/warmup_${i}.log
    done


    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    # formal experiment
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join nonshuffle scale${SCALE}" >> $DIR/logs/noshuffle/scale${SCALE}.log

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
        * )                     usage
    esac
fi