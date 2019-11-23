#!/bin/bash
set -Eeuxo pipefail 

SUBMISSION_DATE=`date "+%Y%m%d-%H%M%S-%N"`

CONFIG_PATH="$1"

if [ ! -f $CONFIG_PATH ]; then
  echo "ERROR: config $CONFIG_PATH does not exists." >&2
  exit 1
fi

NUM_SIMULATIONS="$2"


if [ "$#" -eq 3 ]; then
  GROUP_DESC_SUFFIX="$3"
else
  BASE_CONFIG_PATH=`basename $CONFIG_PATH`
  GROUP_DESC_SUFFIX="${BASE_CONFIG_PATH%.*}"
fi

RUN_CONFIG=`cat "$CONFIG_PATH"`

JOB_GROUP_DIR="/lustre/scratch/grant482/$SUBMISSION_DATE-$GROUP_DESC_SUFFIX/"
mkdir -p $JOB_GROUP_DIR # create for exposition


SIMBAD_HOME="$HOME/SimBaD"
SIMBAD_CLI="$SIMBAD_HOME/build/cli/SimBaD-cli"
SPARK_SUBMIT="$HOME/spark-2.4.0-bin-hadoop2.7/bin/spark-submit"
SPARK_ANALYZER_JAR="$HOME/SimBaD-analyzer/spark/Analyzer/target/scala-2.11/simbad-analyzer_2.11-1.0.jar"

for SEED in `seq 1 $NUM_SIMULATIONS`
do
  JOBDIR=`printf '%sSEED_%05d' ${JOB_GROUP_DIR} $SEED`
  JOBNAME=`printf '%s_%05d' ${GROUP_DESC_SUFFIX} $SEED`
  mkdir -p "$JOBDIR"

  cat << SCRIPT_EOF | qsub \
    -l walltime=48:00:00 \
    -l select=1:ncpus=1:mem=24gb \
    -o "$JOBDIR/stdout.txt" \
    -e "$JOBDIR/stderr.txt" \
    -v "JOBDIR=${JOBDIR},SEED=${SEED}" \
    -N "$JOBNAME" 

  #!/bin/bash
  set -Eeuxo pipefail
  umask 007

  module load boost/1.68.0-gcc6.2.0

  mkdir -p \$JOBDIR && cd \$JOBDIR

  # creates description file
  echo '$RUN_CONFIG' > model_description.simbad

  # simulate
  \time -v $SIMBAD_CLI model_description.simbad --model.parameters.seed=\$SEED | \time -v gzip - > stream.csv.gz 
  touch SIMULATION_SUCCESS  

  # immediate parquet conversion
  module load jdk8/1.8.0_172

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

done

