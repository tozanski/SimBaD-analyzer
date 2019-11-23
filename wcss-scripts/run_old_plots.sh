#!/bin/bash
set -Eeuxo pipefail 

DESC_SUFFIX="$1"
JOBDIR=`readlink -f $1`

# Do the checks before task submission
if [ ! -d $JOBDIR ]; then
  echo "ERROR: directory $JOBDIR does not exists." >&2
  exit 1
#elif [ ! -f "$JOBDIR/muller_data.csv" ]; then
#  echo "ERROR: directory $JOBDIR does not contain a muller_data.csv"
#  exit 2
#elif [ ! -f "$JOBDIR/time_stats/_SUCCESS" ]; then
#  echo "ERROR: directory $JOBDIR does not contain a time_stats/_SUCCESS"
#  exit 3
fi

MULLERPLOTS="$HOME/mullerPlots/mullerplot-gnuplot.py"
MULLERPLOTSPYTHON="$HOME/mullerPlots/mullerplot-matplotlib.py"
TIMESTATS="$HOME/timeStatsPlots/plot_stats.py"
GNUPLOTSCRIPT="$HOME/mullerPlots/gnu_plot.gp" 

cat << SCRIPT_EOF | qsub \
  -l walltime=4:00:00 \
  -l select=1:ncpus=1:mem=8gb \
  -o "$JOBDIR/plots-stdout.txt" \
  -e "$JOBDIR/plots-stderr.txt" \
  -v "JOBDIR=$JOBDIR" \
  -N "Plots-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load gnuplot/5.0.5
module load python/3.6.8-gcc7.3.0

cd $JOBDIR

mkdir -p plots

"$TIMESTATS" -i $JOBDIR/time_stats/part*.csv -o "$JOBDIR/plots/" 
"$MULLERPLOTSPYTHON" -i $JOBDIR/muller_plot_data/part*.csv -o  "$JOBDIR/plots/muller_plot.png"
#"$MULLERPLOTS" -i $JOBDIR/muller_plot_data/part*.csv -o  "$JOBDIR/plots/muller_plot1" -s $GNUPLOTSCRIPT  && rm $JOBDIR/plots/muller_plot.csv
#convert -density 1200 $JOBDIR/plots/muller_plot.csv $JOBDIR/plots/muller_plot.png
#convert $JOBDIR/plots/muller_plot.csv $JOBDIR/plots/muller_plot.pdf

SCRIPT_EOF


