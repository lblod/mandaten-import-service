#!/bin/bash
exec >> /proc/1/fd/1 2>> /proc/1/fd/1 # redirect output to STDOUT from cron

## configuration
DEFAULT_GRAPH=${DEFAULT_GRAPH:-"http://mu.semte.ch/application"}
IMPORT_DIR=${IMPORT_DIR:-"/data/imports"}
KEEP_DATA=${KEEP_DATA:-""}
## functions
function importfile() {
    FILENAME=$1
    if [[ $KEEP_DATA ]]; then
        java -jar /usr/local/bin/import.jar --endpoint "$SPARQL_ENDPOINT" --file "$FILENAME" --graph "$DEFAULT_GRAPH" --keep-data --post-process-queries /data/queries
    else
        java -jar /usr/local/bin/import.jar --endpoint "$SPARQL_ENDPOINT" --file "$FILENAME" --graph "$DEFAULT_GRAPH" --post-process-queries /data/queries
    fi
    if [ $? -eq 0 ];then
        curl -s -X POST $CLEAR_ENDPOINT
    fi
}

function newRecentFile() {
    # find files older than 30min, but less than 24 hours old
    # 30min window assures the export is done writing to the file
    # results are sorted on desc modified time and the first one is retained
    file=`find $IMPORT_DIR -mmin -$((60*24)) -mmin +30 -name '*.ttl' -printf "%T+\t%p\n" | sort -rk 1,1 | head -n 1 | awk '{ print $2 }'`
    echo $file
}

## program
echo "searching for files to import"
file=`newRecentFile`
if [ ! -z $file ]; then
    STARTDATE=`date +%y%m%d-%H%M`
    echo "$STARTDATE: importing $file"
    importfile $file
    if [ $? -eq 0 ]; then
        ENDDATE=`date +%y%m%d-%H%M`
        echo "$ENDDATE: finished importing file"
    else
        echo "$ENDDATE: FAILED importing file"
    fi
fi
