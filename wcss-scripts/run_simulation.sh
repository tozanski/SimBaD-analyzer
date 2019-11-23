#!/bin/bash
set -Eeuxo pipefail 

SUBMISSION_DATE=`date "+%Y%m%d-%H%M%S-%N"`

CONFIG_PATH="$1"

if [ ! -f $CONFIG_PATH ]; then
  echo "ERROR: config $CONFIG_PATH does not exists." >&2
  exit 1
fi

if [ "$#" -eq 2 ]; then
  DESC_SUFFIX="$2"
else
  BASE_CONFIG_PATH=`basename $CONFIG_PATH`
  DESC_SUFFIX="${BASE_CONFIG_PATH%.*}"
fi

CONFIG=`cat "$CONFIG_PATH"`

JOBDIR="/lustre/scratch/grant482/$SUBMISSION_DATE-$DESC_SUFFIX"/

SIMBAD_HOME="$HOME/SimBaD"
SIMBAD_CLI="$SIMBAD_HOME/build/cli/SimBaD-cli"
SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"
CHRONICLES_CONFIG="$SIMBAD_HOME/examples/parametric_evolution_3d_stream_to_chronicle.simbad"


cat << SCRIPT_EOF | qsub \
  -l walltime=48:00:00 \
  -l select=1:ncpus=1:mem=24gb \
  -o "$JOBDIR/stdout.txt" \
  -e "$JOBDIR/stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N $DESC_SUFFIX

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load boost/1.68.0-gcc6.2.0

mkdir -p $JOBDIR && cd $JOBDIR

# creates description file
echo '$CONFIG' > model_description.simbad

\time -v $SIMBAD_CLI model_description.simbad | \time -v gzip - > stream.csv.gz 
touch SIMULATION_SUCCESS  
 
module load jdk8/1.8.0_172
\time -v $SPARK_SUBMIT \
  --master local \
  --class analyzer.StreamReader \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=2g \
  --conf spark.graphx.pregel.checkpointInterval=10 \
  --conf spark.memory.fraction=0.1 \
  --conf spark.local.dir=$TMPDIR/spark \
  $SPARK_ANALYZER_JAR \
  "$JOBDIR/stream.csv.gz" \
  "$JOBDIR/stream.parquet" \
  > stream-converter-spark-stdout.txt 2> stream-converter-spark-stderr.txt

 
#\time -v gunzip -c stream.csv.gz | \time -v $SIMBAD_CLI $CHRONICLES_CONFIG | \time -v gzip - > chronicles.csv.gz
#touch POSTPROCESSING_SUCCESS

SCRIPT_EOF


