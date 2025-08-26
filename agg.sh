#!/bin/bash
# This runs the aggregation periodically
#
if [ -z $LOG_FILE ] ; then
	LOG_FILE=/tmp/agg.log
fi

if [ -z $POLL_TIME ] ; then
        # DEFAULT every 4 hours
	POLL_TIME=14400
fi

while true ; do
        date |tee -a $LOG_FILE
        python -m generate_metag_metat_functional_agg 2>&1 |tee -a $LOG_FILE
        python -m generate_metap_functional_agg 2>&1 |tee -a $LOG_FILE
        echo "Sleeping $POLL_TIME"
        sleep $POLL_TIME
done
