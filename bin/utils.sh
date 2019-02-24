#!/usr/bin/env bash
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/../.." && pwd )"

echo "dir : $DIR"

gen_data(){
    SCL=$1
    cd $DIR/tpch-spark/dbgen
    ./dbgen -s $SCL -T O
    ./dbgen -s $SCL -T L

    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
    mkdir -p data
    mv tpch-spark/dbgen/*.tbl data/

}

move_data(){
    $DIR/alluxio/bin/alluxio fs mkdir $DIR/data
    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/ $DIR/data/

#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/lineitem.tbl /tpch/lineitem.tbl;
#    $DIR/alluxio/bin/alluxio fs copyFromLocal $DIR/data/orders.tbl /tpch/orders.tbl;

}

clean_data(){
    cd $DIR
    if [[ -d data ]]; then
        rm -r data/
    fi
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

load_data(){
    move_data

    cd $DIR

    workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[0]}:/home/ec2-user/
    scp -o StrictHostKeyChecking=no -r data/ ec2-user@${workers[1]}:/home/ec2-user/

    ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"
    ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "/home/ec2-user/alluxio/bin/alluxio fs load --local /home/ec2-user/data/"

}
