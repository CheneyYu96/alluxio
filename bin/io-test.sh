#!/usr/bin/env bash
#set -euxo pipefail
set -x

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

FROM_HDFS=0
NEED_PAR_INFO=1
PER_COL=1

FAULT=0

times=1

con_times=10
tmp_dir=

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

init_alluxio_status(){
    if [[ ! -d $DIR/tpch_parquet ]]; then
        convert_test $scl
    fi

    if [[ `cat ${ALLUXIO_ENV}` == "1" ]]; then
        echo 'Alluxio env already prepared'
    else
        if [[ "${PER_COL}" -eq "0" ]]; then
            bundle_env
        elif [[ "${PER_COL}" -eq "1" ]]; then
            per_col_env
        elif [[ "${PER_COL}" -eq "2" ]]; then
            table_repl_env
        fi

        fr_env
#        $DIR/alluxio/bin/alluxio logLevel --logName=alluxio.master.repl.ReplManager --target=master --level=DEBUG

        move_par_data
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

    init_alluxio_status

    dir_name=$(get_dir_index scale${scl}_query${query}_trace)
    mkdir -p ${dir_name}

    from=${query}
    to=${query}

    if [[ "${query}" -eq "0" ]]; then
        from=1
        to=22
    fi

    for((q=${from};q<=${to};q++)); do
        cd ${DIR}/alluxio

        for((c_tm=1;c_tm<=${con_times};c_tm++)); do
            log_dir_name=${dir_name}/con${c_tm}
            mkdir -p ${log_dir_name}

            python query_scheduler.py ${q} ${log_dir_name} --policy ${PER_COL} --fault ${FAULT} > ${log_dir_name}/master.log 2>&1 &
        done
        wait

    done

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

                java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/${sf}.txt
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
    rm_env

    remove $DIR/logs
    remove $DIR/alluxio/logs

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    worker_num=$(($worker_num-2))

    for i in `seq 0 ${worker_num}`; do
        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "rm /home/ec2-user/alluxio/logs/*"
    done

    ps -aux | grep python | awk '{print $2}' | xargs kill -9
}

rm_env(){
    rm_env_except_pattern
    remove $DIR/alluxio/pattern-gt.txt
}

rm_env_except_pattern(){
    remove $DIR/alluxio_env
    remove $DIR/alluxio/origin-locs.txt
    remove $DIR/replica-locs.txt

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    worker_num=$(($worker_num-2))

    for i in `seq 0 ${worker_num}`; do
        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/*"
    done
}

complie_job(){
    cd $DIR/tpch-spark
    git pull
    sbt assembly
}


bandwidth_test(){
    limit=$1
    qry=$2

    init_alluxio_status

    scl=`cat ${DATA_SCALE}`

    upname=$(get_dir_index band${limit}_qry${qry}_)
    mkdir -p ${upname}

    limit_bandwidth $limit

#    test_bandwidth $DIR/logs

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

con_all_test(){
    qry=$1
    up_times=$2


    for((con_times=1;con_times<=up_times;con_times=con_times+1)); do
        bandwidth_test 1000000 ${qry}
    done
}

con_test(){
    qry=$1
    con_times=$2

    bandwidth_test 1000000 ${qry}
}

compare_test(){
    qry=$1
    times=$2

    for useper in `seq 0 1`; do
        PER_COL=$useper
        policy_test 1000000 $qry
        rm_env
    done
}

policy_test(){
    qry=$1
    up_times=$2

    bdgt=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.budget=' | cut -d "=" -f 2)

    p_dir=$(get_dir_index q${qry}_b${bdgt}_dft_)
    mkdir -p ${p_dir}

    default_env

    con_all_test ${qry} ${up_times}
    mv ${DIR}/logs/band* ${p_dir}

    policy_env
    rm_env

#    warm up
    con_times=1
    bandwidth_test 1000000 ${qry}
    rm -r $DIR/logs/band*

    sleep 300

    p_dir=$(get_dir_index q${qry}_b${bdgt}_plc${PER_COL}_)
    mkdir -p ${p_dir}

    con_all_test ${qry} ${up_times}
    mv ${DIR}/logs/band* ${p_dir}

}

all_policy_test(){
    up_times=$1

    for((qr=1;qr<=22;qr++)); do
        mkdir ${DIR}/logs/q${qr}

        policy_test ${qr} ${up_times}
        rm_env

        mv ${DIR}/logs/q${qr}* ${DIR}/logs/q${qr}
    done

}


#####################
#  call python test script
#####################
limit=5000000
DIST=2

all_query_con_test(){
    rate=$1
    timeout=$2
    query=0

    df_log_dir_name=$(get_dir_index py_q${query}_rt${rate}_dft_)

    default_env
    init_alluxio_status
    limit_bandwidth ${limit}

    cd ${DIR}/alluxio
    python con_query_test.py \
        ${rate} \
        ${timeout} \
        ${query} \
        ${df_log_dir_name} \
        --policy ${PER_COL} \
        --fault ${FAULT} \
        --gt True \
        --dist ${DIST}

    free_limit

    rm_env_except_pattern

    policy_env

    interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
    start=$(date "+%s")

    init_alluxio_status

    now=$(date "+%s")
    tm=$((now-start))

    sleep_time=$((interval+300-tm))

    sleep ${sleep_time} # wait util replication finished

    limit_bandwidth ${limit}

    plc_log_dir_name=$(get_dir_index py_q${query}_rt${rate}_plc${PER_COL}_)

    cd ${DIR}/alluxio
    python con_query_test.py \
        ${rate} \
        ${timeout} \
        ${query} \
        ${plc_log_dir_name} \
        --policy ${PER_COL} \
        --fault ${FAULT} \
        --gt False \
        --dist ${DIST}

    free_limit
}


auto_all_query_test(){
    rate=$1
    timeout=$2
    query=0

    df_log_dir_name=$(get_dir_index py_q${query}_rt${rate}_dft_)

    default_env
    init_alluxio_status
    limit_bandwidth ${limit}

    cd ${DIR}/alluxio
    python con_query_test.py \
        ${rate} \
        ${timeout} \
        ${query} \
        ${df_log_dir_name} \
        --policy ${PER_COL} \
        --fault ${FAULT} \
        --gt True \
        --dist ${DIST}

    free_limit

    for plc in 0 1 2; do
#    for plc in 2; do
        PER_COL=${plc}

        rm_env_except_pattern

        policy_env

        interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
        start=$(date "+%s")

        init_alluxio_status

        now=$(date "+%s")
        tm=$((now-start))

        sleep_time=$((interval+300-tm))

        sleep ${sleep_time} # wait util replication finished

        limit_bandwidth ${limit}

        plc_log_dir_name=$(get_dir_index py_q${query}_rt${rate}_plc${PER_COL}_)

        cd ${DIR}/alluxio
        python con_query_test.py \
            ${rate} \
            ${timeout} \
            ${query} \
            ${plc_log_dir_name} \
            --policy ${PER_COL} \
            --fault ${FAULT} \
            --gt False \
            --dist ${DIST}

        free_limit
    done
}

skew_cmpr_test(){
    rate=$1
    timeout=$2

    PER_COL=0

    for DIST in 0 1 2 3; do
        all_query_con_test ${rate} ${timeout}
        rm_env
    done
}

band_cmpr_test(){
    rate=$1
    timeout=$2

    PER_COL=1

    for band in 5000000; do
        limit=${band}
        all_query_con_test ${rate} ${timeout}
        rm_env
    done
}

rate_auto_test(){
    timeout=$1

    for rt in 20 40 60; do
        rate=${rt}
        auto_all_query_test ${rate} ${timeout}
        mv ${DIR}/logs ${DIR}/r${rt}logs
        rm_env
    done
}

loc_wait=$(cat ../spark/conf/spark-defaults.conf | grep locality | cut -d ' ' -f 2)


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
        rm-env)                 rm_env
                                ;;
        cpjob)                   complie_job
                                ;;
        band-all)               bandwidth_test_all $2 $3
                                ;;
        cmpr)                   compare_test $2 $3
                                ;;
        policy)                 policy_test $2 $3
                                ;;
        policy-all)             all_policy_test $2 $3
                                ;;
        con)                    con_test $2 $3
                                ;;
        con-all)                con_all_test $2 $3
                                ;;
        py-all)                 all_query_con_test $2 $3
                                ;;
        auto)                   auto_all_query_test $2 $3
                                ;;
        skew)                   skew_cmpr_test $2 $3
                                ;;
        band)                   band_cmpr_test $2 $3
                                ;;
        rate)                   rate_auto_test $2
                                ;;
        * )                     usage
    esac
fi