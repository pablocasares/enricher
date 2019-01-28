#!/usr/bin/env bash

if [ $# -lt 1 ];
then
        echo "USAGE: $0 config.json"
        exit 1
fi

CURRENT=`pwd` && cd `dirname $0` && SOURCE=`pwd` && cd ${CURRENT} && PARENT=`dirname ${SOURCE}`

CLASSPATH=${CLASSPATH}:${PARENT}/config
for file in ${PARENT}/lib/*.jar;
do
    CLASSPATH=${CLASSPATH}:${file}
done

[ -z "$JVM_OPTIONS" ] && JVM_OPTIONS="-Djdk.nio.maxCachedBufferSize=262144 -Xmx30m -Xms30m -XX:MaxDirectMemorySize=15m -XX:MaxMetaspaceSize=30m -XX:SurvivorRatio=6 -Xss512k -XX:ReservedCodeCacheSize=15m -XX:NewSize=15m"

exec java ${JVM_OPTIONS} -cp ${CLASSPATH} io.wizzie.enricher.Enricher $1
