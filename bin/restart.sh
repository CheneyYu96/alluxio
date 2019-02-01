#!/usr/bin/env bash
set -euo pipefail


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR

echo "Copy conf dir"

./alluxio copyDir ../conf
./alluxio copyDir ../../spark/conf

echo "Restart alluxio & hdfs"

/home/ec2-user/alluxio/bin/alluxio-stop.sh all;
/home/ec2-user/hadoop/sbin/stop-dfs.sh;
/home/ec2-user/hadoop/sbin/start-dfs.sh;
/home/ec2-user/alluxio/bin/alluxio format;
/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount
