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
elif [ -f "$JOBDIR/chronicles.parquet/_SUCCESS" ]; then
  echo "ERROR: directory $JOBDIT already contains chronicles.paruqet"
  exit 3
fi

SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"

cat << SCRIPT_EOF | qsub \
  -l walltime=12:00:00 \
  -l select=1:ncpus=4:mem=16gb \
  -o "$JOBDIR/chronicler-job-stdout.txt" \
  -e "$JOBDIR/chronicler-job-stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N "chr-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load jdk8/1.8.0_172

cd $JOBDIR

\time $SPARK_SUBMIT \
  --master local \
  --class analyzer.Chronicler \
  --conf spark.executor.cores=4 \
  --conf spark.cores.max=4 \
  --conf spark.driver.memory=3g \
  --conf spark.executor.memory=12g \
  --conf spark.graphx.pregel.checkpointInterval=10 \
  --conf spark.memory.fraction=0.1 \
  --conf spark.local.dir=$TMPDIR/spark \
  $SPARK_ANALYZER_JAR \
  $JOBDIR \
  > chronicler-spark-stdout.txt 2> chronicler-spark-stderr.txt


SCRIPT_EOF


