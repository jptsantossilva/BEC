#!/bin/bash
pid=$(ps aux | grep -i streamlit | awk {'print $2'} )
if ps -p $pid >/dev/null
then
   echo "$pid is running"
   kill -9 $pid
fi
nohup streamlit run pnl.py > pnl.log &