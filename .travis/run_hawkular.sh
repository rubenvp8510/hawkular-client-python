#!/bin/bash

WAIT_STEP=3
MAX_STEPS=120
LOWER_VERSION="0.15"

if [[ -z ${HAWKULAR_VERSION+x} ]]; then
    HAWKULAR_VERSION='latest'
fi

if [[ -z ${HAWKULAR_IMAGE+x} ]]; then
    HAWKULAR_IMAGE="rubensvp/hawkular-metrics"
fi

HAWKULAR_IMAGE=${HAWKULAR_IMAGE}:${HAWKULAR_VERSION}

function metrics_status {
    curl -s http://localhost:8080/hawkular/metrics/status | jq -r '.MetricsService'  2> /dev/null
}

function alerts_status {
    curl -s http://localhost:8080/hawkular/alerts/status | jq -r '.status' 2> /dev/null
}

function cassandra_status {
    docker exec hawkular-cassandra nodetool statusbinary  | tr -dc '[[:print:]]'  2> /dev/null
}

function wait_hawkular {
    METRICS_STATUS=$(metrics_status)
    ALERTS_STATUS=$(alerts_status)
    TOTAL_WAIT=0
    echo "Starting hawkular-metrics:$HAWKULAR_VERSION ..."
    while ([ "$METRICS_STATUS" != "STARTED" ] || ( [ "$ALERTS_STATUS" != "STARTED" ] && [ "$HAWKULAR_VERSION" != "$LOWER_VERSION" ]) )  && [ ${TOTAL_WAIT} -lt ${MAX_STEPS} ]; do
        METRICS_STATUS=$(metrics_status)
        ALERTS_STATUS=$(alerts_status)
        sleep ${WAIT_STEP}
        if [[ "$HAWKULAR_VERSION" == "$LOWER_VERSION" ]]; then
            echo "Hawkular server status, metrics: $METRICS_STATUS"
        else
            echo "Hawkular server status, metrics: $METRICS_STATUS, alerts: $ALERTS_STATUS"
        fi
        TOTAL_WAIT=$((TOTAL_WAIT+WAIT_STEP))
        echo "Waited $TOTAL_WAIT seconds for Hawkular metrics to start."
    done
}

function wait_hawkular_alerts {
    ALERTS_STATUS=$(alerts_status)
    TOTAL_WAIT=0
    echo "Starting hawkular-alerts:$HAWKULAR_VERSION ..."
    while [ "$ALERTS_STATUS" != "STARTED" ] && [ ${TOTAL_WAIT} -lt ${MAX_STEPS} ]; do
        ALERTS_STATUS=$(alerts_status)
        sleep ${WAIT_STEP}
        echo "Hawkular alerts status: $ALERTS_STATUS"
        TOTAL_WAIT=$((TOTAL_WAIT+WAIT_STEP))
        echo "Waited $TOTAL_WAIT seconds for Hawkular alerts to start."
    done
}


function launch_hawkular {
    docker run --name hawkular-metrics -p 8080:8080 --link hawkular-cassandra -d  ${HAWKULAR_IMAGE}
}

function launch_hawkular_alerts {
    docker run --name hawkular-metrics -p 8080:8080 -d  ${HAWKULAR_IMAGE}
}

function launch_cassandra {
    docker run --name  hawkular-cassandra  -e CASSANDRA_START_RPC=true -d cassandra:3.0.13
}

function wait_cassandra {
    CASSANDRA_STATUS=$(cassandra_status)
    TOTAL_WAIT=0;
    while [ "$CASSANDRA_STATUS" != "running" ] && [ ${TOTAL_WAIT} -lt ${MAX_STEPS} ]; do
        CASSANDRA_STATUS=$(cassandra_status)
        echo "Cassandra server status: $CASSANDRA_STATUS."
        sleep ${WAIT_STEP}
        TOTAL_WAIT=$((TOTAL_WAIT+WAIT_STEP))
        echo "Waited $TOTAL_WAIT seconds for Cassandra to start."
    done
}

if [[ "$HAWKULAR_IMAGE" == "hawkular/hawkular-alerts" ]]; then
    launch_hawkular_alerts
    wait_hawkular_alerts
else
    launch_cassandra
    wait_cassandra
    launch_hawkular
    wait_hawkular
fi