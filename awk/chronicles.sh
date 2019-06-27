#!/bin/bash
set -Eeuxo pipefail


# decompress the stream, enumerate events and extract the fields needed to chronicles
cat stream.csv \
    | tail -n +2 \
     | awk -F ";" '
BEGIN{
    print "x ; y ; z ; event_id ; time ;  delta_time ; event_kind ; mutation_id ; be ; br ; le ; lr ; se ; sr"
}
{
    print $1 ";" $2 ";" $3 ";" NR-1 ";" $18 ";" $19 ";" $20  ";" $13 ";" $7 ";" $8 ";" $9 ";" $10 ";" $11 ";" $12 
}' \
    > clean_stream.csv

# sort by position, effectively partitioning the data by position     
{ cat initial.csv && tail -n +2 clean_stream.csv ; } | \
    sort  -k1,3 -t ";" --stable \
    > pos_stream.csv

# for each position extract the birth and death time

awk -F ";" '
BEGIN{
  pid = 0
  x = ""
  y = ""
  z = ""
  prev_time = 0
  prev_ekind = 0
  print "event_id ; particle_id ; parent_id ; birth_time ; death_time ; x ; y ; z ; mut_id ; be ; br ; le ; lr ; se ; sr"
}
{
    # the particle is effecitvely buffered in memory
    time = $5
    ekind = $7

    # outputs the last particle from previous position if the position did not end with death
    if( x != $1 || y != $2 || z != $3 ) # new position
    {
        if( prev_ekind == 1 )
            print  eid ";" pid ";" 0 ";" prev_time ";" "inf" ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "settlers.csv"
        else if( prev_ekind == 4 )
            print  eid ";" pid ";" 0 ";" prev_time ";" "inf" ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "offsprings.csv"

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
            print eid ";" pid ";" pid-1 ";" prev_time ";" time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "settlers.csv"
        else if (prev_ekind == 4) 
              print eid ";" pid ";" pid-1 ";" prev_time ";" time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr > "offsprings.csv"
        
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

    pid++

    prev_time = time
    prev_ekind = ekind
}
END{
    # output last particle if it was no death
    if( prev_eknd != 2)
        print eid ";" pid ";" pid-1 ";" prev_time ";" time ";" x ";" y ";" z ";" mid ";" be ";" br ";" le ";" lr ";" se ";" sr
}
' < pos_stream.csv > linear_chronicles.csv
