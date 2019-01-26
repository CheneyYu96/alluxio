#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

pre_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL

    cd $DIR
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/
}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    alluxio/bin/alluxio fs rm -R /tpch
}

shuffle() {
    SCALE=$1
    mkdir -p  $DIR/logs/shuffle

    pre_data $SCALE
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;

    if [[ `cat ${DIR}/alluxio/conf/threshold` == "1" ]]; then
        echo "0" > ${DIR}/alluxio/conf/threshold
    else
        echo "1" > ${DIR}/alluxio/conf/threshold
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    for i in `seq 1 2`
    do
    {
        $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar 4 > $DIR/logs/shuffle/"$SCALE"_p$i.log
    } &
    done
    wait    

    clean_data
    echo "$( cat $DIR/logs/shuffle/$SCALE.log | grep 'Finish run query')"

}

noshuffle() {
    SCALE=$1
    mkdir -p  $DIR/logs/noshuffle

    pre_data $SCALE

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    # spread data
    for ((i=1;i<=3;i++)); do
    {
        $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar 4 >  /dev/null 2>&1;
    }
    done
    
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"

    # formal experiment
    for i in `seq 1 2`
    do
    {
        $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \ $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar 4 >  $DIR/logs/noshuffle/$SCALE.log

    } &
    done
    wait
    
    clean_data
    echo "$( cat $DIR/logs/noshuffle/$SCALE.log | grep 'Finish run query')"

}

usage() {
    echo "Usage: $0 shffl|noshffl scale"
}


if [[ "$#" -lt 2 ]]; then
    usage
    exit 1
else
    case $1 in
        shffl)                  shuffle $2
                                ;;
        noshffl)                noshuffle $2
                                ;;
        clean)                  clean_data
                                ;;
        * )                     usage
    esac
fi