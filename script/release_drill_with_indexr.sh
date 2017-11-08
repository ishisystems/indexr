#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/..
ROOT_DIR=$(echo $PWD)
TARGET_DIR=${ROOT_DIR}/target/
source ${ROOT_DIR}/script/env.sh

LIBDIR=${ROOT_DIR}/lib
RELEASE_PATH=${ROOT_DIR}/distribution/indexr-${VERSION}

function cp_jar {
    if [ ! -f $1 ]; then
        echo "$1 not exists!"
        exit 1
    fi
    cp -f $1 ${TARGET_DIR}/apache-drill-1.11.0/jars/
}

wget -nc http://www-eu.apache.org/dist/drill/drill-1.11.0/apache-drill-1.11.0.tar.gz
tar -xvf apache-drill-1.11.0.tar.gz -C ${TARGET_DIR}
cd ${TARGET_DIR}/
cp -r ${RELEASE_PATH} apache-drill-1.11.0/
rm -f apache-drill-1.11.0/jars/3rdparty/httpclient-4.2.5.jar apache-drill-1.11.0/jars/3rdparty/httpcore-4.2.4.jar
cp apache-drill-1.11.0/indexr-0.6.1/indexr-drill/jars/3rdparty/* apache-drill-1.11.0/jars/3rdparty/
cp_jar ${MAVEN_PATH}/org/apache/drill/contrib/drill-indexr-storage/1.11.0/drill-indexr-storage-1.11.0.jar
tar -czvf apache-drill-1.11.0.tar.gz apache-drill-1.11.0
