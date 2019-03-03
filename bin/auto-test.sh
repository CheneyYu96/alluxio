#!/usr/bin/env bash
set -euxo pipefail

# This file originates from auto-test-4.sh
# Correct some mistakes in auto-test-4.sh

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

USE_PARQUER=0

check_parquet(){
    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        echo --run-parquet
    fi
}
check_from_hdfs(){
    FROM_HDFS=$1
    if [[ "${FROM_HDFS}" -eq "1" ]]; then
        echo --from-hdfs
    fi
}
move_data_hdfs(){
    $DIR/hadoop/bin/hadoop fs -mkdir -p $DIR/data
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data

    for f in $(ls $DIR/data); do
        $DIR/hadoop/bin/hadoop fs -copyFromLocal $DIR/data/$f $DIR/data/$f
    done

#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl $DIR/data/lineitem.tbl
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl $DIR/data/orders.tbl
}

# shuffle & non shuffle for TPCH
base() {
    SCALE=$1
    QUERY=$2
    FROM_HDFS=$3
    MEM='4g'

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
    if [[ "${FROM_HDFS}" -eq "0" ]]; then
        move_data
    else
        move_data_hdfs
    fi
    clear_workerloads

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        convert ${FROM_HDFS}
    fi

    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --query ${q} \
                $(check_parquet) \
                --app-name "TPCH shuffle: scale${SCALE} query${q}" \
                > $DIR/logs/shuffle/scale${SCALE}_query${q}.log 2>&1 \
                $(check_from_hdfs ${FROM_HDFS})

        collect_workerloads shuffle query${q}
    done

    if [[ "${FROM_HDFS}" -eq "1" ]]; then
        ${DIR}/hadoop/bin/hadoop fs -rm -R /home
    fi
    ${DIR}/alluxio/bin/alluxio fs rm -R /home

    # --------------------nonshuffle--------------------------- #

    nonshuffle_env
    if [[ "${FROM_HDFS}" -eq "0" ]]; then
        move_data
    else
        move_data_hdfs
    fi
    clear_workerloads

    if [[ "${USE_PARQUER}" -eq "1" ]]; then
        convert ${FROM_HDFS}
    fi

    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --query ${q} \
                $(check_parquet) \
                --app-name "TPCH noshuffle: scale${SCALE} query${q}" \
                > $DIR/logs/noshuffle/scale${SCALE}_query${q}.log 2>&1 \
                $(check_from_hdfs ${FROM_HDFS})

        collect_workerloads noshuffle query${q}
    done

    if [[ "${FROM_HDFS}" -eq "1" ]]; then
        ${DIR}/hadoop/bin/hadoop fs -rm -R /home
    fi

    ${DIR}/alluxio/bin/alluxio fs rm -R /home

}

single_test() {
    scl=$1
    query=$2
    dir_name=$(get_dir_index query${query}_scale${scl}_single)
    mkdir -p ${dir_name}

    gen_data $scl
    base ${scl} ${query} 0

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}

all_query() {
    scl=$1
    dir_name=$(get_dir_index scale${scl}_all)
    mkdir -p ${dir_name}

    gen_data $scl
    base ${scl} 0 0

    mv $DIR/logs/noshuffle ${dir_name}
    mv $DIR/logs/shuffle ${dir_name}
    clean_data
}


convert(){
    FROM_HDFS=$1
    $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
            --convert-table $(check_from_hdfs ${FROM_HDFS})
}

par_single_test(){
    scl=$1
    query=$2
    dir_name=$(get_dir_index query${query}_scale${scl}_par_single)
    mkdir -p ${dir_name}

    gen_data $scl
    USE_PARQUER=1
    base ${scl} ${query} 1

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
    base ${scl} 0 1

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

# DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
# DIR="$( cd "$DIR/../.." && pwd )"

# echo "dir : $DIR"

# workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
# echo "worker0: ${workers[0]}"
# echo "worker1: ${workers[1]}"

# pre_data(){
#     SCL=$1
#     cd $DIR/tpch-spark/dbgen
#     ./dbgen -s $SCL -T O
#     ./dbgen -s $SCL -T L

#     cd $DIR
#     mkdir -p data
#     mv tpch-spark/dbgen/*.tbl data/
# }

# clean_data(){
#     cd $DIR
#     if [[ -d data ]]; then
#         rm -r data/
#     fi
#     alluxio/bin/alluxio fs rm -R /tpch
# }

# download_data(){
#     item=$1

#     if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
#         scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${item}/workerLoads0.txt
#         ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
#     fi

#     if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
#         scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${item}/workerLoads1.txt
#         ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
#     fi
# }

# all() {
#     SCALE=$1
#     QUERY=$2 # for further use

#     mkdir -p  $DIR/logs/shuffle
#     mkdir -p  $DIR/logs/noshuffle

#     if [[ ! -d $DIR/data ]]; then
#         pre_data $SCALE
#     fi

#     $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;

#     $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

#     # shuffle
#     echo "----------TESTING SHUFFLE----------"
#     $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join shuffle scale${SCALE}" > $DIR/logs/shuffle/scale${SCALE}.log 2>&1

#     download_data shuffle

#     # noshuffle
#     echo "----------TESTING NO_SHUFFLE----------"
#     # spread data
#     echo "spread data"
    
#     for ((i=1;i<=2;i++)); do
#         $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join warmup${i} scale${SCALE}" > $DIR/logs/noshuffle/warmup_${i}.log 2>&1
#     done

#     if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
#         ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
#     fi

#     if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
#         ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
#     fi

#     # formal experiment
#     $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py --app "Join nonshuffle scale${SCALE}" > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1

#     download_data noshuffle
# }

# usage() {
#     echo "Usage: "
#     echo "$0 all SCALE QUERY"
#     echo "$0 pre SCALE"
#     echo "$0 clean"
# }


# if [[ "$#" -lt 3 ]]; then
#     usage
#     exit 1
# else
#     case $1 in
#         all)                    all $2 $3
#                                 ;;
#         pre)                    pre_data $2
#                                 ;;
#         clean)                  clean_data
#                                 ;;
#         * )                     usage
#     esac
# fi