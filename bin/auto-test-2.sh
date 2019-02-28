#!/usr/bin/env bash
set -euxo pipefail

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

# shuffle & non shuffle for TPCH
base() {
    SCALE=$1
    QUERY=$2

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    from=$QUERY
    to=$QUERY
    if [[ "${QUERY}" -eq "0" ]]; then
        from=1
        to=22
    fi

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=0/c\alluxio.user.file.replication.min=2" $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" $DIR/alluxio/conf/alluxio-site.properties
    ${DIR}/alluxio/bin/restart.sh
    move_data

    clear_workerloads

    # non shuffle
    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
                $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${q} "TPCH: scale${SCALE} query${q}" \
                > $DIR/logs/noshuffle/scale${SCALE}_query${q}.log 2>&1

        collect_workerloads noshuffle query${q}
    done

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

    # ----------------------------------------------- #
    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=2/c\/alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties
    ${DIR}/alluxio/bin/restart.sh
    move_data
    clear_workerloads

    #shuffle
    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
                $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${q} "TPCH: scale${SCALE} query${q}" \
                > $DIR/logs/shuffle/scale${SCALE}_query${q}.log 2>&1

        collect_workerloads shuffle query${q}
    done

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

}

single_test() {
    scl=$1
    query=$2
    dir_name=/home/ec2-user/logs/single_query${query}_scale${scl}
    mkdir -p ${dir_name}
    gen_data $scl

    base ${scl} ${query}

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}

all_query() {
    scl=$1
    dir_name=$(get_dir_index scale${scl}_all)
    mkdir -p ${dir_name}

    gen_data $scl
    base ${scl} 0

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}

limit_test() {
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
        base)                   base $2 $3
                                ;;
        # not available
        #limit)                  limit_test $2 $3
        #                        ;;
        single)                 single_test $2 $3
                                ;;
        all)                    all_query $2
                                ;;
        * )                     usage
    esac
fi