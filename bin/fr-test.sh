#!/usr/bin/env bash
#set -euxo pipefail
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

USE_PARQUER=1
FROM_HDFS=0
NEED_PAR_INFO=0
PER_COL=0
CON_REQ=0


tmp_dir=
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

convert_test(){
    scl=$1
    gen_data $scl

    convert

}

convert(){
    if [[ ! -f ${DATA_SCALE} ]]; then
        touch ${DATA_SCALE}
    fi

    if [[ `cat ${DATA_SCALE}` == "$SCL" && -d $DIR/tpch_parquet ]]; then
        echo "Parquet exist"
    else
        conv_env
        move_data

        $DIR/spark/bin/spark-submit \
            --executor-memory 4g \
            --driver-memory 4g \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
            $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --convert-table \
                $(check_from_hdfs ${FROM_HDFS})
        clean_data

        save_par_data
    fi
}

core=2

# update
all_core=4

default_move_par=0
init_env(){

    NEED_PAR_INFO=1

    if [[ -f $DIR/alluxio/origin-locs.txt ]]; then
        default_move_par=0
    else
        default_move_par=1
    fi

    if [[ ! -f ${ALLUXIO_ENV} ]]; then
        touch ${ALLUXIO_ENV}
    fi

    if [[ `cat ${ALLUXIO_ENV}` == "1" ]]; then
        echo 'Alluxio env already prepared'
    else
        if [[ "${PER_COL}" -eq "1" ]]; then
            per_col_env
        elif [[ "${PER_COL}" -eq "2" ]]; then
            table_repl_env
        elif [[ "${PER_COL}" -eq "3" ]]; then
            bundle_infer_env
        fi

        fr_env
#        $DIR/alluxio/bin/alluxio logLevel --logName=alluxio.master.repl.ReplManager --target=master --level=DEBUG

        if [[ "${default_move_par}" -eq "1" ]]; then
            move_par_data
        else
            java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar write 0 0 ${DIR}/alluxio/origin-locs.txt
        fi

        if [[ "${NEED_PAR_INFO}" -eq "1" ]]; then
             send_par_info
        fi

        echo '1' > ${ALLUXIO_ENV}
    fi

    mkdir -p ${DIR}/logs
}


trace_test(){
    scl=$1
    query=$2

    init_env

    from=$query
    to=$query
    if [[ "${query}" -eq "0" ]]; then
        from=1
        to=22
    fi

    mkdir -p  $DIR/logs/shuffle

    dir_name=$(get_dir_index scale${scl}_query${query}_trace)
    mkdir -p ${dir_name}

    #--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
    #--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
     #--log-trace \

    executor_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    executor_num=$(($executor_num-1))

    total_cores=$[$core*$executor_num]

    for((q=${from};q<=${to};q++)); do
        if [[ "${CON_REQ}" -eq "0" ]]; then
            $DIR/spark/bin/spark-submit \
                --executor-memory 4g \
                --driver-memory 4g \
                --total-executor-cores ${total_cores} \
                --executor-cores ${core} \
                --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
                --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
                --conf spark.locality.wait=${loc_wait} \
                --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                    --query ${q} \
                    $(check_parquet) \
                    --app-name "TPCH shuffle: scale${scl} query${q}" \
                    > $DIR/logs/shuffle/scale${scl}_query${q}.log 2>&1

    #        collect_workerloads shuffle query${q}

            line=$(cat $DIR/logs/shuffle/scale${scl}_query${q}.log | grep 'Got application ID')
            appid=${line##*ID: }
            echo "App ID: ${appid}"

            mkdir -p $DIR/logs/shuffle/query${q}
            collect_worker_logs shuffle/query${q} ${appid}
        else

            concurrent=$[$all_core/$core]
            for((c=1;c<=${concurrent};c++)); do
                $DIR/spark/bin/spark-submit \
                    --executor-memory 2g \
                    --driver-memory 2g \
                    --total-executor-cores ${total_cores} \
                    --executor-cores ${core} \
                    --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
                    --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
                    --conf spark.locality.wait=${loc_wait} \
                    --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                        --query ${q} \
                        $(check_parquet) \
                        --app-name "TPCH shuffle: scale${scl} query${q} con${concurrent}-${c}" \
                        > $DIR/logs/shuffle/scale${scl}_con${c}_query${q}.log 2>&1 &
            done
            wait

#            for((c=1;c<=${concurrent};c++)); do
#                line=$(cat $DIR/logs/shuffle/scale${scl}_con${c}_query${q}.log | grep 'Got application ID')
#                appid=${line##*ID: }
#                echo "App ID: ${appid}"
#
#                mkdir -p $DIR/logs/shuffle/con${c}_query${q}/
#                collect_worker_logs shuffle/con${c}_query${q} ${appid}
#            done
        fi
    done

    mv $DIR/logs/shuffle ${dir_name}
    tmp_dir=${dir_name}

}

trace_range_test(){
    scl=$(cat $DATA_SCALE)
    start=$1
    end=$2

    for((qry=$start;qry<=$end;qry++)); do
        trace_test $scl $qry
    done
}

all_test(){
    scl=$1
    times=$2

    for((t=0;t<${times};t++)); do
        trace_test $scl 0
    done
}

usage() {
    echo "Usage: $0 shffl|noshffl scale #query"
}

INFO_DIR=${DIR}/info

send_par_info(){

    mkdir -p $INFO_DIR

     for f in $(ls $DIR/tpch_parquet); do

        mkdir -p $INFO_DIR/$f

        for sf in $(ls ${DIR}/tpch_parquet/${f}); do

            if [[ "${sf}" != "_SUCCESS" ]]; then
                if [[ ! -f ${INFO_DIR}/${f}/${sf}.txt ]]; then
                    extract_par_info ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/tmp_${sf}.txt ${INFO_DIR}/${f}/${sf}.txt
                fi

                java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar info ${default_move_par} ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/${sf}.txt
            fi

        done
    done

}

extract_par_info(){
    PAR_FILE=$1
    TMP_FILE=$2
    INFO_FILE=$3
    hadoop jar ${DIR}/parquet-mr/parquet-cli/target/parquet-cli-1.12.0-SNAPSHOT-runtime.jar \
        org.apache.parquet.cli.Main column-index ${PAR_FILE} > ${TMP_FILE}

    python ${DIR}/alluxio/offsetParser.py ${TMP_FILE} ${INFO_FILE}

}

clear(){
    del_origin_loc=$1

    remove $DIR/logs
    remove $DIR/alluxio_env
    remove $DIR/alluxio/logs

     if [[ "${del_origin_loc}" -eq "1" ]]; then
         remove $DIR/alluxio/origin-locs.txt
     fi
}

complie_job(){
    cd $DIR/tpch-spark
    git pull
    sbt assembly
}

times=1

bandwidth_test(){
    limit=$1
    qry=$2

    scl=`cat ${DATA_SCALE}`

    upname=$(get_dir_index band${limit}_qry${qry}_)
    mkdir -p ${upname}

    init_env

    limit_bandwidth $limit

    for((t=0;t<${times};t++)); do
        trace_test $scl $qry
        dname[${t}]=${tmp_dir}
    done

    for((t=0;t<${times};t++)); do
        mv ${dname[t]} ${upname}
    done

    free_limit

    tmp_dir=${upname}
}


bandwidth_test_all(){
    limit=$1
    times=$2

    bandwidth_test ${limit} 0
}


compare_test(){
    qry=$1
    times=$2

    for useper in `seq 0 1`; do
        PER_COL=$useper
        policy_test 1000000 $qry
        remove $DIR/alluxio_env
    done
}

policy_test(){
    limit=$1
    qry=$2

    bdgt=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.budget' | cut -d "=" -f 2)

    p_dir=$(get_dir_index q${qry}_b${bdgt}_)
    mkdir -p ${p_dir}


    start=$(date "+%s")

    bandwidth_test ${limit} ${qry}
    first_dir=${tmp_dir}

    now=$(date "+%s")
    tm=$((now-start))
    interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)

    sleep_time=$((interval+180-tm))

    if [[ ${sleep_time} -le 30 ]]; then
        echo "Warning: sleep time not enough. query ${qry}"
        exit 1
    fi

    sleep ${sleep_time}

    bandwidth_test ${limit} ${qry}
    second_dir=${tmp_dir}

    mv ${first_dir} ${p_dir}
    mv ${second_dir} ${p_dir}

}

all_policy_test(){
    limit=$1
    times=$2

    for((qr=1;qr<=22;qr++)); do
        policy_test ${limit} ${qr}
        remove $DIR/alluxio_env
    done

}

loc_wait=$(cat ../spark/conf/spark-defaults.conf | grep locality | cut -d ' ' -f 2)

wait_test(){
    qry=$1
    times=$2

    for wt in 0 5 10 50 100 500 1000 3000 5000 10000; do
        loc_wait=${wt}
        bandwidth_test 1000000 ${qry}
    done
}

wait_time_test(){
    qry=$1
    loc_wait=$2

    bandwidth_test 1000000 ${qry}
}

core_test(){
    qry=$1
    times=$2

    loc_wait=0

    for cr in 1 2 4; do
        core=${cr}
#        compare_test ${qry} ${times}
        bandwidth_test 1000000 ${qry}
    done
}

grained_test(){
    core=$1
    times=$2

#    for qry in 4 6 14 19; do
    for qry in 19; do
        compare_test ${qry} ${times}
    done
}

con_test(){
    core=$1
    times=$2

    loc_wait=0
    CON_REQ=1
    PER_COL=3
#    for qry in 4 6 14 19; do
#        compare_test ${qr} ${times}
#    done
    for((qr=1;qr<=22;qr++)); do
        policy_test 1000000 ${qr}
        remove $DIR/alluxio_env
    done
}

if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        conv)                   convert_test $2
                                ;;
        trace)                  trace_test $2 $3
                                ;;
        trace-range)            trace_range_test $2 $3
                                ;;
        all)                    all_test $2 $3
                                ;;
        clear)                  clear $2
                                ;;
        cpjob)                   complie_job
                                ;;
        band)                   bandwidth_test $2 $3
                                ;;
        band-all)               bandwidth_test_all $2 $3
                                ;;
        cmpr)                   compare_test $2 $3
                                ;;
        policy)                 policy_test $2 $3
                                ;;
        policy-all)             all_policy_test $2 $3
                                ;;
        wait)                   wait_test $2 $3
                                ;;
        wait-time)              wait_time_test $2 $3
                                ;;
        core)                   core_test $2 $3
                                ;;
        grain)                  grained_test $2 $3
                                ;;
        con)                    con_test $2 $3
                                ;;
        * )                     usage
    esac
fi