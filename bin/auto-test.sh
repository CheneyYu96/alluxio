#!/usr/bin/env bash
set -euo pipefail

USER="$( cd "$( dirname "$( dirname "$( dirname "${BASH_SOURCE[0]}" )" )" )"  && pwd )"

pre_data(){
    SCL=$1
    cd $USER/tpch-spark/dbgen;
    ./dbgen -s $SCL;

    cd $USER;
    rm -r data/;
    alluxio/bin/alluxio fs rm -R /tpch;
    mkdir data;
    mv tpch-spark/dbgen/*.tbl data/;
}

shuffle() {
    SCALE=$1
    mkdir -p  $USER/logs/shuffle

    pre_data $SCALE
    $USER/alluxio/bin/alluxio fs copyFromLocal $USER/data/lineitem.tbl /tpch/lineitem.tbl;

    if [[ `cat ${USER}/alluxio/conf/threshold` == "1" ]]; then
        echo "0" > ${USER}/alluxio/conf/threshold
    else
        echo "1" > ${USER}/alluxio/conf/threshold
    fi

    $USER/alluxio/bin/alluxio fs copyFromLocal $USER/data/orders.tbl /tpch/orders.tbl;

    $USER/spark/bin/spark-submit --class "main.scala.TpchQuery" --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $USER/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar 4 >  $USER/logs/shuffle/$SCALE.log

}

noshuffle() {
    pre_data $1
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
        * )                     usage
    esac
fi