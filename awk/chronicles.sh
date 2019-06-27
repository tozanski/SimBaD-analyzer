#!/bin/bash
set -Eeuxo pipefail

STREAM_PATH=$1

INITIAL_PATH=initial.csv

# decompress the stream, enumerate events and extract the fields needed to chronicles
gzcat "$STREAM_PATH" \
    | tail -n +2 \
    | awk -F ";" '
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
}' \
  > clean_stream.csv

#sort by position, effectively partitioning the data by position     
{ cat "$INITIAL_PATH" && tail -n +2 clean_stream.csv ; } | \
    sort  -k1,3 -t ";" --stable \
    > pos_stream.csv

# for each position extract the birth and death time
cat pos_stream.csv | awk -F ";" '
function print_entry(parent_id, death_time){
  if (parent_id)
    print  eid ";" pid ";" parent_id ";" prev_time ";" death_time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "offsprings.csv"
  else
    print  eid ";" pid ";" parent_id ";" prev_time ";" death_time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "settlers.csv"
}
BEGIN{
  pid = 0
  x = ""
  y = ""
  z = ""
  prev_time = 0
  prev_ekind = 0
  prev_parent = 0
  print "event_id ; particle_id ; parent_id ; birth_time ; death_time ; x ; y ; z ; mut_id ; be ; br ; le ; lr ; se ; sr" > "settlers.csv"
  print "event_id ; particle_id ; parent_id ; birth_time ; death_time ; x ; y ; z ; mut_id ; be ; br ; le ; lr ; se ; sr" > "offsprings.csv"
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
' 

#tail -n +2 settlers.csv | sort -k1,1 -t ";" > sorted_settlers.csv &
#tail -n +2 offsprings.csv | sort -k1,1 -t ";" > sorted_offsprings.csv 

#join -t ";" -a 1 -o'0,1.2,2.3,1.4,1.5,1.6,1.7,1.8,1.9,1.10,1.11,1.12,1.13,1.14,1.15,1.16'  sorted_settlers.csv sorted_offsprings.csv > resolved_settlers.csv
join -t ";" -a 1 -o'0,1.2,2.3,1.4,1.5,1.6,1.7,1.8,1.9,1.10,1.11,1.12,1.13,1.14,1.15,1.16'  \
  <(ail -n +2 settlers.csv | sort -k1,1 -t ";") \
  <(tail -n +2 offsprings.csv | sort -k1,1 -t ";") \
  > resolved_settlers.csv
