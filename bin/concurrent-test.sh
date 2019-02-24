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

    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" \
    $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data

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

    sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" \
    $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data

    mkdir -p  $DIR/logs/noshuffle

    # warm up
    for((w=1;w<=5;w++)); do
        $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "warm up ${w}: type${query} scale${scale}" > $DIR/logs/noshuffle/warm_up${w}.log 2>&1

        collect_workerloads noshuffle warmup_${w}
    done

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
    for((scl=6;scl<=18;scl=scl+6)); do #scale
        gen_data $scl

        for((j=0;j<=1;j++)); do #query
            query=$j
            lower_dir=${upper_dir}/type${query}_scale${scl}_con${con_num}_core${core_num}
            mkdir -p ${lower_dir}

            test_bandwidth ${lower_dir}

            con_shuffle ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/shuffle ${lower_dir}

            ${DIR}/alluxio/bin/alluxio fs rm -R /home

            con_nonshuffle ${scl} ${query} ${con_num} ${core_num}
            mv $DIR/logs/noshuffle ${lower_dir}
        done

        clean_data
    done
}

load_data(){
    scl=$1
    gen_data $scl
    move_data

    cd $DIR

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[0]}:/home/ec2-user/
    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[1]}:/home/ec2-user/

    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"

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
        load)                   load_data $2
                                ;;
        * )                     usage
    esac
fi