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
elif [ -f "$JOBDIR/stream.parquet/_SUCCESS" ]; then
  echo "ERROR: directory $JOBDIT already contains stream.parquet file"
  exit 3
fi

SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"

cat << SCRIPT_EOF | qsub \
  -l walltime=48:00:00 \
  -l select=1:ncpus=1:mem=4gb \
  -o "$JOBDIR/stream-converter-job-stdout.txt" \
  -e "$JOBDIR/stream-converter-job-stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N "conv-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load jdk8/1.8.0_172

cd $JOBDIR

$SPARK_SUBMIT \
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

SCRIPT_EOF


