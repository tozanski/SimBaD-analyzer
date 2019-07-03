#!/bin/bash
set -Eeuxo pipefail

if [ $# -ne 2 ];
  then echo "usage chroncicles.sh [stream_path] [initial_path]" >&2
  exit 1
fi

STREAM_PATH=$1
INITIAL_PATH=$2

OUTPUT_PATH=chronciles.csv

SETTLERS_FIFO=`mktemp -u`
OFFSPRINGS_FIFO=`mktemp -u`
RESOLVED_SETTLERS_FIFO=`mktemp -u`

mkfifo -m 600 "$SETTLERS_FIFO" "$OFFSPRINGS_FIFO" "$RESOLVED_SETTLERS_FIFO"

gzcat "$STREAM_PATH" |    # decompress the stream
  tail -n +2 |            # drop header line
  awk -F ";" '            
BEGIN{
  delta = 0
  prev_delta = 0
  eid = 0
  print "x ; y ; z ; event_id ; time ;  delta_time ; event_kind ; mutation_id ; be ; br ; le ; lr ; se ; sr"
}
{
  prev_delta = delta
  delta = $19
  if (prev_delta < delta)
    eid++
    
  print $1 ";" $2 ";" $3 ";" eid ";" $18 ";" $19 ";" $20  ";" $13 ";" $7 ";" $8 ";" $9 ";" $10 ";" $11 ";" $12 
}' |                                      # enumerate events and extract the fields needed to chronicles
  tail -n +2 |                            # drop headers from cleaned stream
  cat "$INITIAL_PATH" - |                 # include initial configuration
  sort  -k1,3 -t ";" --stable |     # sort by position, effectively partitioning the data by position
  awk -F ";" -v OFFSPRINGS_FILE="$OFFSPRINGS_FIFO" -v SETTLERS_FILE="$SETTLERS_FIFO" '
function print_entry(parent_id, death_time){
  if (parent_id)  # parent != 0 is offspring
    print  eid ";" pid ";" parent_id ";" prev_time ";" death_time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > OFFSPRINGS_FILE
  else            # parent == 0 is settler
    print  eid ";" pid ";" parent_id ";" prev_time ";" death_time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > SETTLERS_FILE
}
BEGIN{
  pid = 0
  x = ""
  y = ""
  z = ""
  prev_time = 0
  prev_ekind = 0
  prev_parent = 0

  print "event_id ; particle_id ; parent_id ; birth_time ; death_time ; x ; y ; z ; mut_id ; be ; br ; le ; lr ; se ; sr" > OFFSPRINGS_FILE
  print "event_id ; particle_id ; parent_id ; birth_time ; death_time ; x ; y ; z ; mut_id ; be ; br ; le ; lr ; se ; sr" > SETTLERS_FILE
}
{
  # the particle is effecitvely buffered in memory
  time = $5
  ekind = $7

  # outputs the last particle from previous position if the position did not end with death
  if( x != $1 || y != $2 || z != $3 ) # new position
  {
    if( prev_ekind == 1 )
      print_entry(0, "inf")
    else if( prev_ekind == 4 )
      print_entry(prev_parent, "inf")

    if( ekind == 2 )
      print "WTF-2 " 
    else if( ekind == 4 )
      print "WTF-4 " 
  }
  else # old position
  {
    if( ekind == 1 )
      print "WTF-1"
          
    else if (prev_ekind == 1)
      print_entry(0, time)
    else if (prev_ekind == 4)
      print_entry(prev_parent, time)        
  }

  # buffer values 
  x = $1
  y = $2
  z = $3
  
  eid = $4
  dtime = $6
  mid = $8
  be = $9
  br = $10
  le = $11
  lr = $12
  se = $13
  sr = $14

  prev_time = time
  prev_ekind = ekind
  prev_parent = pid

  if (ekind != 2)
    pid++
    
}
END{
  # output last particle if it was no death
  if (prev_ekind == 1)
    print_entry(0, "inf")
  else if (prev_ekind == 4)
    print_entry(prev_parent, "inf") 
}
' &

join -t ";" -a 1 -o'0,1.2,2.3,1.4,1.5,1.6,1.7,1.8,1.9,1.10,1.11,1.12,1.13,1.14,1.15,1.16'  \
  <(tail -n +2 "$SETTLERS_FIFO" | sort -k1,1 -t ";") \
  <(tee "$OUTPUT_PATH" < "$OFFSPRINGS_FIFO" | tail -n +2 "$OFFSPRINGS_FIFO" | sort -k1,1 -t ";") \
  >> "$OUTPUT_PATH"

rm "$SETTLERS_FIFO" "$OFFSPRINGS_FIFO" "$RESOLVED_SETTLERS_FIFO"