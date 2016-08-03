#!/bin/bash
lastpid=1

lastalive=`ps $lastpid | grep skynet | wc -l`

if [ $lastalive -ge 1 ]; then
    kill $lastalive
fi 

function start()
{
    nohup ./skynet 3g_game/config.login &

    lastpid=$!
}

start
while [ 1 ]; do
    sleep_seconds = 5
    alive=`ps $lastpid | grep skynet | wc -l`

    if [ $lastalive -lt 1 ] then
        nohup curl "http://apis.haoservice.com/sms/send?mobile=18676670538&tpl_id=1773&tpl_value=&key=0a84fdc47c734dd290a7bb06460a1df9" &
        sleep_seconds = 60
        start
    fi
    
    sleep $sleep_seconds
done

