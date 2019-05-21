# /bin/sh
set -euxo pipefail

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"  && pwd )"
DIR="$( cd "$DIR/.." && pwd )"
PARQUET_CLI_DIR="$( cd "$DIR/../parquet-mr/parquet-cli" && pwd )"

echo "Working directory is $DIR"

prepare(){
    cd $PARQUET_CLI_DIR
    mvn clean install -DskipTests
    mvn dependency:copy-dependencies
}

send(){
    FILE_PATH=$1
    cd $PARQUET_CLI_DIR
    java -cp 'target/*:target/dependency/*' org.apache.parquet.cli.Main column-index $FILE_PATH > $DIR/output.txt
    # $DIR/../hadoop-2.8.5/bin/hadoop jar target/parquet-cli-1.12.0-SNAPSHOT-runtime.jar org.apache.parquet.cli.Main column-index $FILE_PATH > $DIR/output.txt
    cd $DIR
    python3.7 offsetParser.py output.txt
}

usage(){
    echo "Usage:\n $0 send <path-to-parquet>"
    echo "$0 prepare"
}

if [[ "$#" -lt 2 ]]; then
    usage
    exit 1
else
    case $1 in
        prepare)                prepare
                                ;;
        send)                   send $2
                                ;;
        * )                     usage
    esac
fi