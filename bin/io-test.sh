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

SIZE=$((1073741824))
check_from_hdfs(){
    if [[ "${FROM_HDFS}" -eq "1" ]]; then
        echo --from-hdfs
    fi
}

convert_test(){
    scl=$1
    size=$2
    gen_data ${scl}

    convert ${size}

}

convert(){
    size=$1

    if [[ ! -f ${DATA_SCALE} ]]; then
        touch ${DATA_SCALE}
    fi

    if [[ `cat ${DATA_SCALE}` == "$SCL" && -d $DIR/tpch_parquet ]]; then
        echo "Parquet exist"
    else
        conv_env ${size}
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
USE_PATTERN=1
THRT=0

init_alluxio_status(){

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
        if [[ "${PER_COL}" -eq "0" ]]; then
            bundle_env
        elif [[ "${PER_COL}" -eq "1" ]]; then
            per_col_env
        elif [[ "${PER_COL}" -eq "2" ]]; then
            table_repl_env
        elif [[ "${PER_COL}" -eq "3" ]]; then
            bundle_infer_env
        fi


        if [[ "${USE_PATTERN}" -eq "1" ]]; then
            sed -i '/^fr.repl.budget.access=/cfr.repl.budget.access=true' $DIR/alluxio/conf/alluxio-site.properties
        else
            sed -i '/^fr.repl.budget.access=/cfr.repl.budget.access=false' $DIR/alluxio/conf/alluxio-site.properties
        fi

        size_in_mb=$((SIZE/1048576))
        fr_env ${size_in_mb}
#        $DIR/alluxio/bin/alluxio logLevel --logName=alluxio.master.repl.ReplManager --target=master --level=DEBUG

        if [[ "${default_move_par}" -eq "1" ]]; then
            move_par_data
        else
            java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar write ${THRT} 0 ${DIR}/alluxio/origin-locs.txt
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

    ${DIR}/alluxio/bin/alluxio fs rm -R '/home'
    ${DIR}/alluxio/bin/alluxio fs rm -R '/fr_dir'

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

    sleep_time=$((interval+180-tm))

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
    PER_COL=3

#    run_default 30 ${timeout}

#    rm_env_except_pattern

#    for bdgt in "0.5" "1" "2"; do
    for bdgt in "2"; do
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

overhead_test(){

    PER_COL=3
    USE_PATTERN=0

#    for bdgt in `seq 1 5`; do
    for bdgt in 1; do
        sed -i "/^fr.repl.budget=/cfr.repl.budget=${bdgt}" ${DIR}/alluxio/conf/alluxio-site.properties

        log_name=$(get_dir_index oh_b${bdgt}_)
        mkdir -p ${log_name}

        sed -i '/^fr.repl.interval=/cfr.repl.interval=300' $DIR/alluxio/conf/alluxio-site.properties

        interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
        start=$(date "+%s")

        cd ${DIR}/alluxio

        init_alluxio_status

        now=$(date "+%s")
        tm=$((now-start))

        timeout=$((interval-tm))
        timeout=$((timeout-30))
        timeout=$((timeout/60))

        if [[ ${timeout} -le 0 ]]; then
            echo "non-positive timeout. bdgt ${bdgt}"
            exit 1
        fi

        df_log_dir_name=$(get_dir_index py_q${query}_rt${rate}_dft_)
        python con_query_test.py \
            20 \
            ${timeout} \
            0 \
            ${df_log_dir_name} \
            --policy 2 \
            --fault ${FAULT} \
            --gt False \
            --dist ${DIST} \
            --log False

        sleep_time=$((bdgt*20+150))
        sleep ${sleep_time}

        mv $DIR/alluxio/logs/master.log ${log_name}


#        rm -r ${df_log_dir_name}
#        rm_env
#        remove $DIR/alluxio/logs
    done
}

alpha_test(){
    PER_COL=3
#    for num in `seq 1 10`; do
    for num in 3; do
        file_num=$((num*1000))

        log_dir_name=$(get_dir_index alpha_f${num}_)
        mkdir -p ${log_dir_name}

        default_env
        init_alluxio_status

        java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar alpha ${file_num} 10

        mv $DIR/alluxio/logs/master.log ${log_dir_name}
        remove $DIR/alluxio_env
        remove $DIR/alluxio/logs
    done

}


thrt_env(){
    THRT=1

    sed -i "/^fr.repl.throttle=/cfr.repl.throttle=true" ${DIR}/alluxio/conf/alluxio-site.properties
    sed -i "/^alluxio.user.file.writetype.default=/calluxio.user.file.writetype.default=CACHE_THROUGH" ${DIR}/alluxio/conf/alluxio-site.properties
    sed -i "/^alluxio.worker.memory.size=/calluxio.worker.memory.size=1GB" ${DIR}/alluxio/conf/alluxio-site.properties
}
throttle_test(){
    rate=$1
    PER_COL=$2

    timeout=18

#    USE_PATTERN=0

    thrt_env

    sed -i "/^fr.repl.budget=/cfr.repl.budget=0.5" ${DIR}/alluxio/conf/alluxio-site.properties

#    run_default ${rate} ${timeout}

    for PER_COL in 1 2 3; do
        rm_env_except_pattern
        ${DIR}/alluxio/bin/alluxio fs rm -R '/home'
        ${DIR}/alluxio/bin/alluxio fs rm -R '/fr_dir'

        run_policy ${rate} ${timeout}
    done

#    remove $DIR/alluxio_env

#    default_env
#    init_alluxio_status

#    java -jar ${DIR}/alluxio/writeparquet/target/writeparquet-2.0.0-SNAPSHOT.jar write ${THRT} 1 ${DIR}/replica-locs.txt
#    sleep 30

}

straggler_test(){
    timeout=$1
    PER_COL=3

#    run_default 30 ${timeout}

    rm_env_except_pattern

    sed -i "/^fr.repl.budget=/cfr.repl.budget=0.5" ${DIR}/alluxio/conf/alluxio-site.properties

#    for PER_COL in 1 2 3; do
        policy_env

        interval=$(cat $DIR/alluxio/conf/alluxio-site.properties | grep 'fr.repl.interval' | cut -d "=" -f 2)
        start=$(date "+%s")

        init_alluxio_status

        now=$(date "+%s")
        tm=$((now-start))

        sleep_time=$((interval+300-tm))

        sleep ${sleep_time} # wait util replication finished

        limit_bandwidth ${limit}

        plc_log_dir_name=$(get_dir_index strg_plc${PER_COL}_)

        cd ${DIR}/alluxio
        python con_query_test.py \
            30 \
            ${timeout} \
            0 \
            ${plc_log_dir_name} \
            --policy ${PER_COL} \
            --fault ${FAULT} \
            --gt False \
            --dist ${DIST} \
            --strg True

        free_limit

        rm_env_except_pattern
#    done
}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        conv)                   convert_test $2 $3
                                ;;
        clear)                  clear $2 $3
                                ;;
        init)                   init_alluxio_status
                                ;;
        rm-env)                 rm_env
                                ;;
        cpjob)                   complie_job
                                ;;
        py-all)                 all_query_con_test $2 $3
                                ;;
        auto)                   auto_all_query_test $2 $3
                                ;;
        band)                   band_cmpr_test $2 $3
                                ;;
        rate)                   rate_auto_test $2
                                ;;
        skew-band)              skew_band_test $2
                                ;;
        spec)                   spec_test $2
                                ;;
        overhead)               overhead_test
                                ;;
        alpha)                  alpha_test
                                ;;
        thrt)                   throttle_test $2 $3
                                ;;
        strg)                   straggler_test $2
                                ;;
        * )                     usage
    esac
fi