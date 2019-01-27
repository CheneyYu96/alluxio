#!/usr/bin/env bash
set -euxo pipefail
set -x

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
    QUERY=$2
    mkdir -p  $DIR/logs/shuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;

    if [[ `cat ${DIR}/alluxio/conf/threshold` == "1" ]]; then
        echo "0" > ${DIR}/alluxio/conf/threshold
    else
        echo "1" > ${DIR}/alluxio/conf/threshold
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/shuffle/scale${SCALE}.log
#    for ((i=1;i<=3;i++)); do
#        $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/shuffle/scale${SCALE}_$i.log
#    done

#    clean_data

}

noshuffle() {
    SCALE=$1
    QUERY=$2

    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    # spread data
    echo '3' > $DIR/tpch-spark/times
    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
    $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} >  /dev/null 2>&1;

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"

    # formal experiment
    echo '1' > $DIR/tpch-spark/times
    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/noshuffle/scale${SCALE}.log

#    for ((i=1;i<=3;i++)); do
#        $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/noshuffle/scale${SCALE}_$i.log
#    done

#    clean_data
#    echo "$( cat $DIR/logs/noshuffle/$SCALE.log | grep 'Finish run query')"

}

usage() {
    echo "Usage: $0 shffl|noshffl scale #query"
}


if [[ "$#" -lt 3 ]]; then
    usage
    exit 1
else
    case $1 in
        shffl)                  shuffle $2 $3
                                ;;
        noshffl)                noshuffle $2 $3
                                ;;
        pre)                    pre_data $2
                                ;;
        clean)                  clean_data
                                ;;
        * )                     usage
    esac
fi