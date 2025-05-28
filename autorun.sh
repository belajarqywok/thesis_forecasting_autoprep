#!/bin/bash
LOGDIR="logfile"
BASELOG="$LOGDIR/data_preparation.log"
mkdir -p "$LOGDIR"

get_logfile() {
  if [ ! -f "$BASELOG" ]; then
    echo "$BASELOG"
  else
    i=1
    while [ -f "$LOGDIR/data_preparation_$i.log" ]; do
      i=$((i+1))
    done
    echo "$LOGDIR/data_preparation_$i.log"
  fi
}

date_now=$(date +%d)
date_now=$((10#$date_now + 1))

if [ "$date_now" -eq 28 ]; then
  GEN_NEW_DATA="True"
else
  GEN_NEW_DATA="False"
fi

LOGFILE=$(get_logfile)
for (( i=1; i<=$1; i++ ))
do
  echo "---------------------------------------------------------" | tee -a "$LOGFILE"
  echo "------------------ [ Iteration - $i ] ------------------"  | tee -a "$LOGFILE"
  echo "---------------------------------------------------------" | tee -a "$LOGFILE"
  python main.py --gen_new_data=$GEN_NEW_DATA \
    --process=SYNC --ranking_by=HEAD_RANK \
    --ranking_number=50 2>&1 | tee -a "$LOGFILE"
done

echo "---------------------------------------------------------" | tee -a "$LOGFILE"
echo "-------------------- [ Historicals ] --------------------" | tee -a "$LOGFILE"
echo "---------------------------------------------------------" | tee -a "$LOGFILE"
ls -al indonesia_stocks/historicals | tee -a "$LOGFILE"

echo "---------------------------------------------------------" | tee -a "$LOGFILE"
echo "--------------------- [ Indicators ] --------------------" | tee -a "$LOGFILE"
echo "---------------------------------------------------------" | tee -a "$LOGFILE"
ls -al indonesia_stocks/indicators | tee -a "$LOGFILE"

echo "---------------------------------------------------------" | tee -a "$LOGFILE"
echo "--------------------- [ Min-Max ] --------------------" | tee -a "$LOGFILE"
echo "---------------------------------------------------------" | tee -a "$LOGFILE"
ls -al indonesia_stocks/min_max | tee -a "$LOGFILE"

