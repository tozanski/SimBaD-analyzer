#!/bin/bash
set -Eeuxo pipefail 

DESC_SUFFIX="$1"
JOBDIR=`readlink -f $1`

if [ ! -d $JOBDIR ]; then
  echo "ERROR: directory $JOBDIR does not exists." >&2
  exit 1
fi

SIMBAD_HOME="$HOME/SimBaD"
SIMBAD_CLI="$SIMBAD_HOME/build/cli/SimBaD-cli"
CHRONICLES_CONFIG="$SIMBAD_HOME/examples/parametric_evolution_3d_stream_to_chronicle.simbad"

cat << SCRIPT_EOF | qsub \
  -l walltime=48:00:00 \
  -l select=1:ncpus=1:mem=32gb \
  -o "$JOBDIR/postproc-stdout.txt" \
  -e "$JOBDIR/postproc-stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N "postproc-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load boost/1.68.0-gcc6.2.0

cd $JOBDIR

if [ -f "$JOBDIR/SIMULATION_SUCCESS" ]; then
  \time -v gunzip -c stream.csv.gz | \time -v $SIMBAD_CLI $CHRONICLES_CONFIG | \time -v gzip - > chronicles.csv.gz
else # recovery - skips the last row
  set +o pipefail # the first gunzip is allowed to fail as stream ile might be incomplete
  \time -v gunzip -c stream.csv.gz | \time -v head -n -1 |  \time -v $SIMBAD_CLI $CHRONICLES_CONFIG | \time -v gzip - > chronicles.csv.gz
  set -o pipefail	
fi
  
touch POSTPROCESSING_SUCCESS

SCRIPT_EOF


