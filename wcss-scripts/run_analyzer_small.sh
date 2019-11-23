#!/bin/bash
set -Eeuxo pipefail 

DESC_SUFFIX="$1"
JOBDIR=`readlink -f $1`

# Do the checks before task submission
if [ ! -d $JOBDIR ]; then
  echo "ERROR: directory $JOBDIR does not exists." >&2
  exit 1
elif [ ! -f "$JOBDIR/stream.csv.gz" ]; then
  echo "ERROR: directory $JOBDIR does not contain a stream.csv.gz"
  exit 2
elif [ ! -f "$JOBDIR/SIMULATION_SUCCESS" ]; then
  echo "ERROR: directory $JOBDIT does not contain SIMULATION_SUCCESS file"
  exit 3
fi

SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"

cat << SCRIPT_EOF | qsub \
  -l walltime=4:00:00 \
  -l select=1:ncpus=4:mem=12gb \
  -o "$JOBDIR/analyzer-stdout.log" \
  -e "$JOBDIR/analyzer-stderr.log" \
  -v "JOBDIR=$JOBDIR" \
  -N "ans-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load jdk8/1.8.0_172

cd $JOBDIR
mkdir -p output_data

rm -rf spark-warehouse

\time -v $SPARK_SUBMIT \
  --master local \
  --class analyzer.Analyzer \
  --conf spark.executor.cores=4 \
  --conf spark.cores.max=4 \
  --conf spark.driver.memory=3g \
  --conf spark.executor.memory=8g \
  --conf spark.graphx.pregel.checkpointInterval=10 \
  --conf spark.memory.fraction=0.1 \
  --conf spark.local.dir=$TMPDIR/spark \
  $SPARK_ANALYZER_JAR \
  "$JOBDIR/stream.parquet" \
  "$JOBDIR/output_data" \
  > spark-analyzer-stdout.log 2> spark-analyzer-stderr.log

rm -rf checkpoints/
rm -rf output_data/checkpoints/
rm -rf spark-warehouse

touch ANALYZER_SUCCESS

SCRIPT_EOF


