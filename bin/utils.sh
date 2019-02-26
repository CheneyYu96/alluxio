#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

gen_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    # ./dbgen -s $SCL -T O
    ./dbgen -s $SCL -T L

    # awk -v FS='|' -v OFS='|' -v ORS='|\n' '{sub(/.{1}$/,"")}1 {print $5, $6, $7, $8, $9, $10, $11}' lineitem.tbl > lineitem_part.tbl
    if $DIR/alluxio/bin/alluxio fs test -e $DIR/data/; then
        echo "exist"
    else
        $DIR/alluxio/bin/alluxio fs mkdir $DIR/data
    fi
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/tpch-spark/dbgen/lineitem.tbl $DIR/data/lineitem.tbl
    $DIR/spark/bin/spark-submit --master spark://$(cat /home/ec2-user/hadoop/conf/masters):7077 --executor-memory 4g /home/ec2-user/tpch-spark/query/cut_table.py --app "cut_table"
    $DIR/alluxio/bin/alluxio fs copyToLocal $DIR/data/lineitem_part.tbl $DIR/tpch-spark/dbgen/lineitem_part.tbl

    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/

}

move_data(){
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl $DIR/data/lineitem.tbl
    # $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl $DIR/data/orders.tbl
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem_part.tbl $DIR/data/lineitem_part.tbl
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    $DIR/alluxio/bin/alluxio fs rm -R $DIR/data/

}

free_limit(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; echo test"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; echo test"
}

limit_bandwidth(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)
    limit=$1
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $limit -u $limit"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sudo wondershaper -c -a eth0; sudo wondershaper -a eth0 -d $limit -u $limit"
}

test_bandwidth() {
    saved_dir=$1

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`);

    echo "Setup iperf3"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "iperf3 -s > /dev/null 2>&1 &"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "iperf3 -c ${workers[0]} -d" >> ${saved_dir}/bandwith.txt

    echo "kill iperf3 server process"
    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "pkill iperf3"
}

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

clear_workerloads(){
    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    if ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi

    if ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no test -e /home/ec2-user/logs/workerLoads.txt; then
        ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "rm /home/ec2-user/logs/workerLoads.txt"
    fi
}

load_data(){
    move_data

    cd $DIR

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[0]}:/home/ec2-user/
    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[1]}:/home/ec2-user/

    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"

}
