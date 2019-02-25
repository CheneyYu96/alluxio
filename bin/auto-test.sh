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
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/partial_table.py \
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
        --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/query/partial_table.py \
        --query ${query} --app "noshuffle: type${query} scale${scale} concurrency${c}" > $DIR/logs/noshuffle/scale${scale}_con${c}.log 2>&1 &
    done
    wait

    collect_workerloads noshuffle noshuffle

}

concurrent_test(){
    con_num=$1
    core_num=$2
    upper_dir=/home/ec2-user/logs
    for((scl=3;scl<=5;scl=scl+2)); do #scale
        gen_data $scl

        for((j=3;j<=4;j++)); do #query
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
        auto)                   auto_test
                                ;;
        mice)                   mice_test $2
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