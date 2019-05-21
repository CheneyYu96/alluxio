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
        shuffle_env
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
        shuffle_env
        move_par_data
        clear_workerloads
        echo '1' > ${ALLUXIO_ENV}
    fi

            #--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
            #--conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file://$DIR/tpch-spark/log4j.properties" \
             #--log-trace \
    for((q=${from};q<=${to};q++)); do
        $DIR/spark/bin/spark-submit \
            --executor-memory 4g \
            --driver-memory 4g \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar \
                --query ${q} \
                $(check_parquet) \
                --app-name "TPCH shuffle: scale${scl} query${q}" \
                > $DIR/logs/shuffle/scale${scl}_query${q}.log 2>&1

        collect_workerloads shuffle query${q}

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

usage() {
    echo "Usage: $0 shffl|noshffl scale #query"
}


send_par_info(){
    java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar fileparquet fileinfo
}


extract_par_info(){
    PAR_FILE=$1
    hadoop jar ${DIR}/parquet-mr/parquet-cli/target/parquet-cli-1.12.0-SNAPSHOT-runtime.jar \
        org.apache.parquet.cli.Main column-index ${PAR_FILE}

}

move_par_jar_to_spark(){
    PREFIX=$DIR/spark/jars
    BACKUP=$DIR/spark/backup
    mkdir -p $BACKUP

    for f in $(ls $DIR/alluxio/jars); do
        mv ${PREFIX}/${f} ${BACKUP}
        mv ${DIR}/alluxio/jars/${f} ${PREFIX}
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
        move-jar)               move_par_jar_to_spark
                                ;;
        * )                     usage
    esac
fi