#!/usr/bin/env bash
set -euxo pipefail

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

# include shuffle & non shuffle
all() {
    SCALE=$1
    QUERY=$2
    MEM=$3
    NUM=$4
#    total_cores=$[$CORES*2]

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

#    $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${CORES} \
#    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
#    --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

    #shuffle

    $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --query ${QUERY} --app "shuffle query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

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

        $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${QUERY} --app "warmup${i} query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/noshuffle/warmup_${i}.log 2>&1

        if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
            scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/noshuffle/workerLoads0_warm${i}.txt
            ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
        fi

        if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
            scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/noshuffle/workerLoads1_warm${i}.txt
            ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
        fi

    done


    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    # formal experiment
    $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --query ${QUERY} --app "noshuffle query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1

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

mice_test() {
    scl=2
    dir=/home/ec2-user/logs/mice_test
    mkdir -p ${dir}
    gen_data $scl

    move_data $scl
    all ${scl} 0 "4g" 2

    mv $DIR/logs/noshuffle ${dir}
    mv $DIR/logs/shuffle ${dir}
    ${DIR}/alluxio/bin/alluxio fs rm -R /tpch
    clean_data
}

all_query() {
    scl=$1
    upper_dir=/home/ec2-user/logs
    mkdir -p ${upper_dir}
    memory=4
    for((scl=12;scl<=18;scl=scl+6)); do #scale
        gen_data $scl
#        for((memory=8;memory<=12;memory=memory+4)); do
            for((j=0;j<=1;j++)); do #query
                query=$j
                lower_dir=${upper_dir}/type${query}_scale${scl}_mem${memory}
                mkdir -p ${lower_dir}

                ${DIR}/alluxio/bin/restart.sh
                move_data $scl
                test_bandwidth ${lower_dir}

                all ${scl} ${query} "${memory}g" 2
                mv $DIR/logs/noshuffle ${lower_dir}
                mv $DIR/logs/shuffle ${lower_dir}
                ${DIR}/alluxio/bin/alluxio fs rm -R /tpch

            done
#        done
        clean_data
     done
}

auto_test() {
    bandwidth=$1
    cores=$2

    free_limit
    export SPARK_WORKER_CORES=${cores}
    upper_dir=/home/ec2-user/logs/cpu${cores}_bandwidth${bandwidth}
    mkdir -p ${upper_dir}

    for((j=1;j<=10;j++)); do
        scl=$j

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
        auto)                   all_query $2 $3
                                ;;
        mice)                   mice_test
                                ;;
        * )                     usage
    esac
fi