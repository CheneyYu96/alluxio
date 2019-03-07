#!/usr/bin/env bash
set -euo pipefail


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

echo "Copy conf dir"

./alluxio copyDir ../conf
./alluxio copyDir ../../spark/conf

workers=(`cat /home/ec2-user/hadoop/conf/slaves`)

ssh ec2-user@${workers[0]} -o StrictHostKeyChecking=no "sed -i '/alluxio.worker.hostname=localhost/c\alluxio.worker.hostname=${workers[0]}' /home/ec2-user/alluxio/conf/alluxio-site.properties"
ssh ec2-user@${workers[1]} -o StrictHostKeyChecking=no "sed -i '/alluxio.worker.hostname=localhost/c\alluxio.worker.hostname=${workers[1]}' /home/ec2-user/alluxio/conf/alluxio-site.properties"


echo "Restart alluxio & hdfs"

/home/ec2-user/alluxio/bin/alluxio-stop.sh all;
/home/ec2-user/hadoop/sbin/stop-dfs.sh;
/home/ec2-user/hadoop/sbin/start-dfs.sh;
/home/ec2-user/alluxio/bin/alluxio format;
/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount
