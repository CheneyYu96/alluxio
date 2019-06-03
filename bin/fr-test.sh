#!/usr/bin/env bash
#set -euxo pipefail
set -x

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
        non_fr_env
        move_data

        $DIR/spark/bin/spark-submit \
            --executor-memory 4g \
            --driver-memory 4g \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
            $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --convert-table \
                $(check_from_hdfs ${FROM_HDFS})

#       test one row group per part file
#        $DIR/spark/bin/spark-submit \
#            --executor-memory 4g \
#            --driver-memory 4g \
#            --master local \
#            $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
#                --convert-table \
#                $(check_from_hdfs ${FROM_HDFS})
        clean_data

        save_par_data
    fi
}

trace_test(){
    scl=$1
    query=$2

    dir_name=$(get_dir_index scale${scl}_query${query}_trace)
    mkdir -p ${dir_name}

    gen_data $scl
    convert

    USE_PARQUER=1
    NEED_PAR_INFO=1

    from=$query
    to=$query
    if [[ "${query}" -eq "0" ]]; then
        from=1
        to=22
    fi

    mkdir -p  $DIR/logs/shuffle

    if [[ `cat ${ALLUXIO_ENV}` == "1" ]]; then
        echo 'Alluxio env already prepared'
    else
        fr_env
#        $DIR/alluxio/bin/alluxio logLevel --logName=alluxio.master.repl.ReplManager --target=master --level=DEBUG

        move_par_data
        if [[ "${NEED_PAR_INFO}" -eq "1" ]]; then
             send_par_info
        fi

        echo '1' > ${ALLUXIO_ENV}
    fi

            #--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
            #--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
             #--log-trace \

    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --executor-memory 4g \
            --driver-memory 4g \
            --conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
            --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
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
    done

    mv $DIR/logs/shuffle ${dir_name}

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

        IDX=0
        for sf in $(ls ${DIR}/tpch_parquet/${f}); do

            if [[ "${sf}" != "_SUCCESS" ]]; then
                 extract_par_info ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/tmp_${IDX}.txt ${INFO_DIR}/${f}/${IDX}.txt
                java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/${IDX}.txt
                ((IDX=IDX+1))
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
    remove $DIR/logs
    remove $DIR/alluxio_env
    remove $DIR/alluxio/logs
}

complie_job(){
    cd $DIR/tpch-spark
    git pull
    sbt assembly
}

bandwidth_test(){
    limit=$1
    times=$2

    scl=`cat ${DATA_SCALE}`

    qry=9

    mkdir -p  $DIR/logs

    limit_bandwidth $limit

    test_bandwidth $DIR/logs

    for((t=0;t<${times};t++)); do
        trace_test $scl $qry
    done

    free_limit
}


bandwidth_test_all(){
    limit=$1
    times=$2

    scl=`cat ${DATA_SCALE}`

    mkdir -p  $DIR/logs

    limit_bandwidth $limit

    test_bandwidth $DIR/logs
    all_test $scl $times

    free_limit
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
        clear)                  clear
                                ;;
        comp)                   complie_job
                                ;;
        band)                   bandwidth_test $2 $3
                                ;;
        band-all)               bandwidth_test_all $2 $3
                                ;;
        * )                     usage
    esac
fi