#!/usr/bin/env bash
set -euxo pipefail

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

USE_PARQUER=0

check_parquet(){
    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        echo --run-parquet
    fi
}

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

    # --------------------shuffle--------------------------- #

    shuffle_env
    move_data
    clear_workerloads

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        convert
    fi

    #shuffle
    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --query ${q} \
                $(check_parquet) \
                --app-name "TPCH shuffle: scale${SCALE} query${q}" \
                > $DIR/logs/shuffle/scale${SCALE}_query${q}.log 2>&1

        collect_workerloads shuffle query${q}
    done

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

    # --------------------nonshuffle--------------------------- #

    nonshuffle_env
    move_data
    clear_workerloads

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        convert
    fi

    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --query ${q} \
                $(check_parquet) \
                --app-name "TPCH noshuffle: scale${SCALE} query${q}" \
                > $DIR/logs/noshuffle/scale${SCALE}_query${q}.log 2>&1

        collect_workerloads noshuffle query${q}
    done

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

}

single_test() {
    scl=$1
    query=$2
    dir_name=$(get_dir_index query${query}_scale${scl}_single)
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


convert(){
    $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
            --convert-table
}

par_single_test(){
    scl=$1
    query=$2
    dir_name=$(get_dir_index query${query}_scale${scl}_par_single)
    mkdir -p ${dir_name}

    gen_data $scl
    USE_PARQUER=1
    base ${scl} ${query}

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}

par_all_test(){
    scl=$1
    dir_name=$(get_dir_index par_scale${scl}_all)
    mkdir -p ${dir_name}

    gen_data $scl
    USE_PARQUER=1
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
        par-single)             par_single_test $2 $3
                                ;;
        par-all)                par_all_test $2
                                ;;
        * )                     usage
    esac
fi