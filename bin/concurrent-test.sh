#!/usr/bin/env bash
set -euxo pipefail

LOCAL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"

source ${LOCAL_DIR}/utils.sh

con_shuffle(){
    scale=$1
    query=$2
    concurrent=$3
    core=$4

    total_cores=$[$core*2]

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=2/c\alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data
    clear_workerloads

    mkdir -p $DIR/logs/shuffle

    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${core} \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "shuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/shuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads shuffle shuffle
}

con_nonshuffle(){
    scale=$1
    query=$2
    concurrent=$3
    core=$4

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.replication.min=0/c\alluxio.user.file.replication.min=2" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data
    clear_workerloads

    mkdir -p $DIR/logs/noshuffle

    # formal experiment
    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${core} \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "noshuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/noshuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads noshuffle noshuffle

}

concurrent_test(){
    con_num=$1
    core_num=$2
    upper_dir=/home/ec2-user/logs
    for((scl=12;scl<=12;scl=scl+6)); do #scale
        gen_data $scl

        for((j=0;j<=1;j++)); do #query
            query=$j
            lower_dir=${upper_dir}/type${query}_scale${scl}_con${con_num}_core${core_num}
            mkdir -p ${lower_dir}

            test_bandwidth ${lower_dir}

            con_shuffle ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/shuffle ${lower_dir}


            con_nonshuffle ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/noshuffle ${lower_dir}
        done

        clean_data
    done
}

con_shuffle_debug() {
    scale=$1
    query=$2
    concurrent=$3
    core=$4

    total_cores=$[$core*2]

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties

    sed -i "/alluxio.user.file.replication.min=2/c\alluxio.user.file.replication.min=0" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data
    clear_workerloads

    mkdir -p $DIR/logs/shuffle

    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit --total-executor-cores ${total_cores} --executor-cores ${core} \
        --conf spark.driver.extraJavaOptions="-agentpath:/home/ec2-user/async-profiler/build/libasyncProfiler.so=start,threads,interval=99700000,raw,event=cpu,file=/home/ec2-user/shuffle-profile-${c}.traces" \
        --conf spark.executor.extraJavaOptions="-agentpath:/home/ec2-user/async-profiler/build/libasyncProfiler.so=start,threads,interval=99700000,raw,event=cpu,file=/home/ec2-user/shuffle-profile-${c}.traces" \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "shuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/shuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads shuffle shuffle
}

con_noshuffle_debug() {
    scale=$1
    query=$2
    concurrent=$3
    core=$4

    sed -i \
    "/alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy/c\alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.RoundRobinPolicy" \
    $DIR/alluxio/conf/alluxio-site.properties
    sed -i "/alluxio.user.file.replication.min=0/c\alluxio.user.file.replication.min=2" $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data
    clear_workerloads

    mkdir -p $DIR/logs/noshuffle

    # formal experiment
    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit \
            --total-executor-cores ${total_cores} \
            --executor-cores ${core} \
            --conf spark.driver.extraJavaOptions="-agentpath:/home/ec2-user/async-profiler/build/libasyncProfiler.so=start,threads,interval=99700000,raw,event=cpu,file=/home/ec2-user/noshuffle-profile-${c}.traces" \
            --conf spark.executor.extraJavaOptions="-agentpath:/home/ec2-user/async-profiler/build/libasyncProfiler.so=start,threads,interval=99700000,raw,event=cpu,file=/home/ec2-user/noshuffle-profile-${c}.traces" \
            --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
                $DIR/tpch-spark/query/join.py \
                    --query ${query} \
                    --app "noshuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/noshuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads noshuffle noshuffle
}

concurrent_test_debug() {
    con_num=$1
    core_num=$2
    upper_dir=/home/ec2-user/logs
    for((scl=12;scl<=12;scl=scl+6)); do #scale
        gen_data $scl

        for((j=0;j<=1;j++)); do #query
            query=$j
            lower_dir=${upper_dir}/type${query}_scale${scl}_con${con_num}_core${core_num}
            mkdir -p ${lower_dir}

            test_bandwidth ${lower_dir}

            con_shuffle_debug ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/shuffle ${lower_dir}


            con_nonshuffle_debug ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/noshuffle ${lower_dir}
        done

        clean_data
    done
}

auto_test(){
    for((con=1;con<=16;con=con*2)); do
        cores=$[16/$con]
        concurrent_test ${con} ${cores}
    done
}

mice_test(){
    scl=$1
    dir_name=/home/ec2-user/logs/mice-test

    gen_data $scl

    mkdir -p ${dir_name}

    con_shuffle ${scl} 0 4 4
    mv $DIR/logs/shuffle ${dir_name}

    con_nonshuffle ${scl} 0 4 4
    mv $DIR/logs/noshuffle ${dir_name}

    clean_data
}


usage() {
    echo "Usage: $0 test # concurrency # cores"
}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        test)                   concurrent_test $2 $3
                                ;;
        debug-test)             concurrent_test_debug $2 $3
                                ;;
        auto)                   auto_test
                                ;;
        mice)                   mice_test $2
                                ;;
        * )                     usage
    esac
fi