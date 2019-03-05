#!/usr/bin/env bash
set -euxo pipefail

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

USE_PARQUER=0
FROM_HDFS=0

check_parquet(){
    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        echo --run-parquet
    fi
}

check_from_hdfs(){
    if [[ "${FROM_HDFS}" -eq "1" ]]; then
        echo --from-hdfs
    fi
}

# shuffle & non shuffle for TPCH
base() {
    SCALE=$1
    QUERY=$2

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        gen_data $SCALE
    fi

    from=$QUERY
    to=$QUERY
    if [[ "${QUERY}" -eq "0" ]]; then
        from=1
        to=22
    fi

    # --------------------shuffle--------------------------- #

    shuffle_env

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        move_par_data
    fi

    if [[ "${USE_PARQUER}" -eq "0" ]]; then
        move_data
    fi

    clear_workerloads
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

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        move_par_data
    fi

    if [[ "${USE_PARQUER}" -eq "0" ]]; then
        move_data
    fi

    clear_workerloads
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

convert_test(){
    scl=$1
    gen_data $scl

    convert
}

convert(){
    nonshuffle_env
    move_data

    $DIR/spark/bin/spark-submit \
        --executor-memory 4g \
        --driver-memory 4g \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
            --convert-table \
            $(check_from_hdfs ${FROM_HDFS})

    save_par_data
}

micro(){
   scl=$1
   dir_name=$(get_dir_index scale${scl}_micro)

   gen_data $scl

   mkdir -p ${dir_name}/query30
   base ${scl} 30
   mv $DIR/logs/noshuffle ${dir_name}/query30
   mv $DIR/logs/shuffle ${dir_name}/query30

   mkdir -p ${dir_name}/query31
   base ${scl} 31
   mv $DIR/logs/noshuffle ${dir_name}/query31
   mv $DIR/logs/shuffle ${dir_name}/query31

   clean_data
}

par_micro(){
   scl=$1
   dir_name=$(get_dir_index scale${scl}_par_micro)

   gen_data $scl
   convert
   USE_PARQUER=1

   mkdir -p ${dir_name}/query30
   base ${scl} 30
   mv $DIR/logs/noshuffle ${dir_name}/query30
   mv $DIR/logs/shuffle ${dir_name}/query30

   mkdir -p ${dir_name}/query31
   base ${scl} 31
   mv $DIR/logs/noshuffle ${dir_name}/query31
   mv $DIR/logs/shuffle ${dir_name}/query31

   clean_data
}

par_single_test(){
    scl=$1
    query=$2
    dir_name=$(get_dir_index query${query}_scale${scl}_par_single)
    mkdir -p ${dir_name}

    USE_PARQUER=1
#    FROM_HDFS=1

    gen_data $scl
    convert

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
    convert

    USE_PARQUER=1
    base ${scl} 0

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}

limit_test() {
    scl=$1
    bandwidth=$2

    free_limit
    dir_name=$(get_dir_index par_scale${scl}_bandwidth${bandwidth}_all)
    mkdir -p ${dir_name}

    gen_data $scl
    convert
    USE_PARQUER=1

    limit_bandwidth ${bandwidth}
    test_bandwidth ${dir_name}

    base ${scl} 0

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}

    clean_data
    free_limit
}

limit_micro() {
    scl=$1
    bandwidth=$2

    free_limit
    dir_name=$(get_dir_index par_scale${scl}_bandwidth${bandwidth}_micro)
    mkdir -p ${dir_name}

    gen_data $scl
    convert
    USE_PARQUER=1

    limit_bandwidth ${bandwidth}
    test_bandwidth ${dir_name}

    mkdir -p ${dir_name}/query30
    base ${scl} 30
    mv $DIR/logs/noshuffle ${dir_name}/query30
    mv $DIR/logs/shuffle ${dir_name}/query30

    mkdir -p ${dir_name}/query31
    base ${scl} 31
    mv $DIR/logs/noshuffle ${dir_name}/query31
    mv $DIR/logs/shuffle ${dir_name}/query31

    clean_data
    free_limit
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
        limit)                  limit_test $2 $3
                                ;;
        limit-micro)            limit_micro $2 $3
                                ;;
        single)                 single_test $2 $3
                                ;;
        all)                    all_query $2
                                ;;
        par-single)             par_single_test $2 $3
                                ;;
        par-all)                par_all_test $2
                                ;;
        par-micro)              par_micro $2
                                ;;
        micro)                  micro $2
                                ;;
        conv)                   convert_test $2
                                ;;
        * )                     usage
    esac
fi