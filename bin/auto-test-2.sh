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

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=0/c\/alluxio.user.file.replication.min=2" $DIR/alluxio/conf/alluxio-site.properties
#    sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" $DIR/alluxio/conf/alluxio-site.properties
    ${DIR}/alluxio/bin/restart.sh
    move_data

#    load_data
    clear_workerloads
#    spread data
#    for ((i=1;i<=4;i++)); do
#
#        $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
#        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
#        --query ${QUERY} --app "warmup${i} query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/noshuffle/warmup_${i}.log 2>&1
#
#        collect_workerloads noshuffle warmup_${i}
#
#    done

    # formal experiment
    $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --query ${QUERY} --app "noshuffle query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1

    collect_workerloads noshuffle noshuffle

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=2/c\/alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties
#    sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" $DIR/alluxio/conf/alluxio-site.properties
    ${DIR}/alluxio/bin/restart.sh
    move_data

#    $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${CORES} \
#    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
#    --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

    #shuffle
    clear_workerloads
    $DIR/spark/bin/spark-submit --num-executors ${NUM} --driver-memory ${MEM} --executor-memory ${MEM} \
    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
    --query ${QUERY} --app "shuffle query type${QUERY} scale${SCALE} mem${MEM}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

    collect_workerloads shuffle shuffle

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

}

mice_test() {
    scl=$1
    dir=/home/ec2-user/logs/mice_test
    mkdir -p ${dir}
    gen_data $scl

    all ${scl} 0 "4g" 2

    mv $DIR/logs/noshuffle ${dir}
    mv $DIR/logs/shuffle ${dir}
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
                ${DIR}/alluxio/bin/alluxio fs rm -R /home

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
        mice)                   mice_test $2
                                ;;
        * )                     usage
    esac
fi