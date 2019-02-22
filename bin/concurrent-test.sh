#!/usr/bin/env bash
set -euxo pipefail

source auto-test-2.sh

collect_workerloads(){
    worker_log_dir=$1
    name=$2

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${worker_log_dir}/workerLoads0_${name}.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/${worker_log_dir}/workerLoads1_${name}.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
}


con_shuffle(){
    scale=$1
    query=$2
    concurrent = $3

    sed -i "/alluxio.user.file.passive.cache.enabled=true/c\alluxio.user.file.passive.cache.enabled=false" \
    $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data

    mkdir -p $DIR/logs/shuffle

    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "shuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/shuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads shuffle shuffle
}

con_nonshuffle(){
    scale=$1
    query=$2
    concurrent = $3

    sed -i "/alluxio.user.file.passive.cache.enabled=false/c\alluxio.user.file.passive.cache.enabled=true" \
    $DIR/alluxio/conf/alluxio-site.properties

    ${DIR}/alluxio/bin/restart.sh
    move_data

    mkdir -p  $DIR/logs/noshuffle

    # warm up
    for((w=1;w<=3;w++)); do
        $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "warm up ${w}: type${query} scale${scale}" > $DIR/logs/noshuffle/warm_up${w}.log 2>&1

        collect_workerloads noshuffle warmup_${w}
    done

    # formal experiment
    for((c=1;c<=${concurrent};c++)); do
        $DIR/spark/bin/spark-submit \
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/join.py \
        --query ${query} --app "noshuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/noshuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads noshuffle noshuffle

}

concurrent_test(){
    con_num = $1
    upper_dir=/home/ec2-user/logs
    for((scl=12;scl<=18;scl=scl+6)); do #scale
        gen_data $scl

        for((j=0;j<=1;j++)); do #query
            query=$j
            lower_dir=${upper_dir}/type${query}_scale${scl}_con${con_num}
            mkdir -p ${lower_dir}

            test_bandwidth ${lower_dir}

            con_shuffle ${scl} ${query} ${con_num}
            mv $DIR/logs/shuffle ${lower_dir}

            ${DIR}/alluxio/bin/alluxio fs rm -R /tpch

            con_nonshuffle ${scl} ${query} ${con_num}
            mv $DIR/logs/noshuffle ${lower_dir}
        done

        clean_data
    done
}


usage() {
    echo "Usage: $0 test # concurrency"
}


if [[ "$#" -lt 2 ]]; then
    usage
    exit 1
else
    case $1 in
        test)                   concurrent_test $2
                                ;;
        * )                     usage
    esac
fi