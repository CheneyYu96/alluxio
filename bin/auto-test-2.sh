#!/usr/bin/env bash
set -euxo pipefail
set -x

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

pre_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL -T O
    ./dbgen -s $SCL -T L

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

#    if [[ `cat ${DIR}/alluxio/conf/threshold` == "1" ]]; then
#        echo "0" > ${DIR}/alluxio/conf/threshold
#    else
#        echo "1" > ${DIR}/alluxio/conf/threshold
#    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

#    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --executor-memory 2g --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/shuffle/scale${SCALE}.log
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/py/join.py > $DIR/logs/shuffle/scale${SCALE}.log

    $DIR/alluxio/bin/alluxio fs rm -R /tpch
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"

    if [[ ! -d $DIR/tpch_out ]]; then
        rm -r /home/ec2-user/tpch_out/
    fi

}

# include shuffle !!
noshuffle() {
    SCALE=$1
    QUERY=$2

    mkdir -p  $DIR/logs/shuffle
    mkdir -p  $DIR/logs/noshuffle

    if [[ ! -d $DIR/data ]]; then
        pre_data $SCALE
    fi

    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

    #shuffle
#    echo '1' > $DIR/tpch-spark/times
#    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --executor-memory 2g --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#    $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/shuffle/scale${SCALE}.log

    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/py/join.py > $DIR/logs/shuffle/scale${SCALE}.log

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads0.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads1.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"

    if [[ ! -d $DIR/tpch_out ]]; then
        rm -r /home/ec2-user/tpch_out/
    fi

    # spread data
#    echo '2' > $DIR/tpch-spark/times
#    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --executor-memory 2g --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#    $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} >  $DIR/logs/noshuffle/warnup.log 2>&1

    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/py/join.py >> $DIR/logs/noshuffle/warmup.log
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/py/join.py >> $DIR/logs/noshuffle/warmup.log

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt; rm -r /home/ec2-user/tpch_out/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt; rm -r /home/ec2-user/tpch_out/"

    if [[ ! -d $DIR/tpch_out ]]; then
        rm -r /home/ec2-user/tpch_out/
    fi

    # formal experiment
#    echo '1' > $DIR/tpch-spark/times
#    $DIR/spark/bin/spark-submit --class "main.scala.TpchQuery" --executor-memory 2g --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 \
#        $DIR/tpch-spark/target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar ${QUERY} > $DIR/logs/noshuffle/scale${SCALE}.log 2>&1
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 $DIR/tpch-spark/py/join.py >> $DIR/logs/noshuffle/scale${SCALE}.log

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[0]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads0.txt
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        scp -o StrictHostKeyChecking=no ec2-user@${workers[1]}:/home/ec2-user/logs/workerLoads.txt /home/ec2-user/logs/shuffle/workerLoads1.txt
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm -r /home/ec2-user/tpch_out/"

    if [[ ! -d $DIR/tpch_out ]]; then
        rm -r /home/ec2-user/tpch_out/
    fi

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