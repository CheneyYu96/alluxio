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

default_move_par=1

init_alluxio_status(){

    if [[ ! -d $DIR/tpch_parquet ]]; then
        convert_test $scl
    fi

    if [[ -f $DIR/alluxio/origin-locs.txt ]]; then
        default_move_par=0
    else
        default_move_par=1
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
        elif [[ "${PER_COL}" -eq "3" ]]; then
            bundle_infer_env
        fi

        fr_env
#        $DIR/alluxio/bin/alluxio logLevel --logName=alluxio.master.repl.ReplManager --target=master --level=DEBUG

        if [[ "${default_move_par}" -eq "1" ]]; then
            move_par_data
        else
            java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar ${DIR}/alluxio/origin-locs.txt
        fi

        if [[ "${NEED_PAR_INFO}" -eq "1" ]]; then
             send_par_info
        fi

        echo '1' > ${ALLUXIO_ENV}
    fi

    mkdir -p ${DIR}/logs
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

                java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar ${default_move_par} ${DIR}/tpch_parquet/${f}/${sf} ${INFO_DIR}/${f}/${sf}.txt
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
    clear_origin=$1
    clear_pattern=$2
    ps -aux | grep python | awk '{print $2}' | xargs kill -9

    rm_env_except_pattern
    if [[ "${clear_pattern}" -eq "1" ]]; then
        remove $DIR/alluxio/pattern-gt.txt
    fi

    remove $DIR/logs
    remove $DIR/alluxio/logs

    if [[ "${clear_origin}" -eq "1" ]]; then
        remove $DIR/alluxio/origin-locs.txt
    fi

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    worker_num=(`cat /home/ec2-user/hadoop/conf/slaves | wc -l`)
    worker_num=$(($worker_num-2))

    for i in `seq 0 ${worker_num}`; do
        ssh ec2-user@${workers[$i]} -o StrictHostKeyChecking=no "rm /home/ec2-user/alluxio/logs/*"
    done

}

rm_env(){
    rm_env_except_pattern
    remove $DIR/alluxio/pattern-gt.txt
}

rm_env_except_pattern(){
    remove $DIR/alluxio_env
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


#####################
#  call python test script
#####################
limit=1000000
DIST=0

run_default(){
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
        --policy 2 \
        --fault ${FAULT} \
        --gt True \
        --dist ${DIST}

    free_limit
}

run_policy(){
    rate=$1
    timeout=$2
    query=0

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

all_query_con_test(){
    rate=$1
    timeout=$2

    run_default ${rate} ${timeout}

    rm_env_except_pattern

    run_policy ${rate} ${timeout}

}


auto_all_query_test(){
    rate=$1
    timeout=$2

    run_default ${rate} ${timeout}

    for plc in 0 1 2; do
#    for plc in 2; do
        PER_COL=${plc}

        rm_env_except_pattern

        run_policy ${rate} ${timeout}

    done
}

skew_cmpr_test(){
    rate=$1
    timeout=$2

    PER_COL=0

#    for DIST in 0 1 2 3; do
    for DIST in 0 1 2; do
        all_query_con_test ${rate} ${timeout}

        mkdir -p $DIR/logs/skew_${DIST}
        mv $DIR/logs/py* $DIR/logs/skew_${DIST}
        mv $DIR/alluxio/logs/master.log $DIR/logs/skew_${DIST}/

        rm_env
        remove $DIR/alluxio/logs

    done
}

band_cmpr_test(){
    rate=$1
    timeout=$2
    query=0

    run_default ${rate} ${timeout}

    PER_COL=1

    rm_env_except_pattern

    policy_env

    interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
    start=$(date "+%s")

    init_alluxio_status

    now=$(date "+%s")
    tm=$((now-start))

    sleep_time=$((interval+300-tm))

    sleep ${sleep_time} # wait util replication finished

    for band in 1000000 3000000 5000000; do
        limit=${band}

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

rate_auto_test(){
    timeout=$1

    for rt in 20 30 40; do

        run_default ${rt} ${timeout}

        for bdgt in "0.5" "1" "2"; do

            sed -i "/^fr.repl.budget=/cfr.repl.budget=${bdgt}" ${DIR}/alluxio/conf/alluxio-site.properties

            mkdir -p $DIR/logs/r${rt}_b${bdgt}

            for plc in 0 1 2 3; do
                PER_COL=${plc}

                rm_env_except_pattern

                run_policy ${rate} ${timeout}

                mv $DIR/alluxio/logs/master.log $DIR/logs/r${rt}_b${bdgt}/master_${plc}.log
                remove $DIR/alluxio/logs
            done

            mv $DIR/logs/py_q0_rt${rt}_plc* $DIR/logs/r${rt}_b${bdgt}
            cp -r $DIR/logs/py_q0_rt${rt}_dft* $DIR/logs/r${rt}_b${bdgt}

        done

        rm_env
        rm -r $DIR/logs/py_q0_rt${rt}_dft*

    done
}

loc_wait=$(cat ../spark/conf/spark-defaults.conf | grep locality | cut -d ' ' -f 2)


skew_band_test(){
    timeout=$1

    sed -i "/^fr.repl.budget=/cfr.repl.budget=1" ${DIR}/alluxio/conf/alluxio-site.properties

    for rt in 20 30 40; do

        run_default ${rt} ${timeout}

        PER_COL=1

        mkdir -p $DIR/logs/band_r${rt}

        for band in 1000000 3000000 5000000; do
            limit=${band}

            rm_env_except_pattern

            run_policy ${rt} ${timeout}
        done

        mv $DIR/logs/py_q0_rt${rt}_plc* $DIR/logs/band_r${rt}
        cp -r $DIR/logs/py_q0_rt${rt}_dft* $DIR/logs/band_r${rt}

        remove $DIR/alluxio/logs

        PER_COL=0
        mkdir -p $DIR/logs/skew_r${rt}

        for DIST in 0 1 2; do

            rm_env_except_pattern

            run_policy ${rt} ${timeout}

            mv $DIR/alluxio/logs/master.log $DIR/logs/skew_r${rt}/master_${DIST}.log
            remove $DIR/alluxio/logs
        done

        mv $DIR/logs/py_q0_rt${rt}_plc* $DIR/logs/skew_r${rt}
        mv $DIR/logs/py_q0_rt${rt}_dft* $DIR/logs/skew_r${rt}

        rm_env

    done


}

spec_test(){
    timeout=$1
    PER_COL=0

#    run_default 30 ${timeout}

#    rm_env_except_pattern

#    for bdgt in "0.5" "1" "2"; do
    for bdgt in "1"; do
        sed -i "/^fr.repl.budget=/cfr.repl.budget=${bdgt}" ${DIR}/alluxio/conf/alluxio-site.properties

        policy_env

        interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
        start=$(date "+%s")

        init_alluxio_status

        now=$(date "+%s")
        tm=$((now-start))

        sleep_time=$((interval+300-tm))

        sleep ${sleep_time} # wait util replication finished

#        for rt in 20 30 40; do
        for rt in 40; do

            mkdir -p $DIR/logs/r${rt}_b${bdgt}

#            limit_bandwidth ${limit}

            plc_log_dir_name=$(get_dir_index py_q0_rt${rt}_plc${PER_COL}_)

            cd ${DIR}/alluxio
            python con_query_test.py \
                ${rt} \
                ${timeout} \
                0 \
                ${plc_log_dir_name} \
                --policy ${PER_COL} \
                --fault ${FAULT} \
                --gt False \
                --dist ${DIST}

#            free_limit
            mv $DIR/logs/py_q0_rt${rt}_plc* $DIR/logs/r${rt}_b${bdgt}
        done

        rm_env_except_pattern
    done

}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        conv)                   convert_test $2
                                ;;
        clear)                  clear $2 $3
                                ;;
        rm-env)                 rm_env
                                ;;
        cpjob)                   complie_job
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
        skew-band)              skew_band_test $2
                                ;;
        spec)                   spec_test $2
                                ;;
        * )                     usage
    esac
fi