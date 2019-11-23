#!/bin/bash
set -Eeuxo pipefail 

DESC_SUFFIX="$1"
JOBDIR=`readlink -f $1`

# Do the checks before task submission
if [ ! -d $JOBDIR ]; then
  echo "ERROR: directory $JOBDIR does not exists." >&2
  exit 1
elif [ ! -f "$JOBDIR/chronicles.csv.gz" ]; then
  echo "ERROR: directory $JOBDIR does not contain a chronicles.csv.gz"
  exit 2
elif [ ! -f "$JOBDIR/POSTPROCESSING_SUCCESS" ]; then
  echo "ERROR: directory $JOBDIR does not contain POSTPROCESSING_SUCCESS file"
  exit 3
fi

SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"

cat << SCRIPT_EOF | qsub \
  -l walltime=24:00:00 \
  -l select=1:ncpus=1:mem=4gb \
  -o "$JOBDIR/spark-converter-stdout.txt" \
  -e "$JOBDIR/spark-converter-stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N "converter-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load jdk8/1.8.0_172

cd $JOBDIR

\time $SPARK_SUBMIT \
  --master local \
  --class analyzer.Converter \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=1 \
  --conf spark.driver.memory=1g \
  --conf spark.executor.memory=1g \
  --conf spark.local.dir=$TMPDIR/spark \
  $SPARK_ANALYZER_JAR \
  $JOBDIR \
  > converter-stdout.txt 2> converter-stderr.txt


SCRIPT_EOF


