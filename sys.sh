#!/bin/bash


function check_disk()
{
    disk=`df -h | grep /home | awk '{print $5}' | cut -f 1 -d "%"`

    if [ $disk -gt 90 ]; then
        nohup curl "http://apis.haoservice.com/sms/send?mobile=18676670538&tpl_id=1773&tpl_value=&key=0a84fdc47c734dd290a7bb06460a1df9" &    
    fi
}

function check_mem()
{
    mem=`free | grep Mem | awk '{print $4}'`
    if [ $mem -lt 200000 ]; then
        nohup curl "http://apis.haoservice.com/sms/send?mobile=18676670538&tpl_id=1773&tpl_value=&key=0a84fdc47c734dd290a7bb06460a1df9" &
    fi
}

function check_cpu()
{
    cpu=`vmstat 2 3 | awk 'BEGIN{n=0} {if(NR>0){n=n+$15}} END{print n}'`
    if [ $cpu -lt 20 ];
        nohup curl "http://apis.haoservice.com/sms/send?mobile=18676670538&tpl_id=1773&tpl_value=&key=0a84fdc47c734dd290a7bb06460a1df9" &
    fi
}

