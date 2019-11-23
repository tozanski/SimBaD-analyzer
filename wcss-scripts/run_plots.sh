#!/bin/bash
set -Eeuxo pipefail 

DESC_SUFFIX="$1"
JOBDIR=`readlink -f $1`

# Do the checks before task submission
if [ ! -d $JOBDIR ]; then
  echo "ERROR: directory $JOBDIR does not exists." >&2
  exit 1
elif [ ! -d "$JOBDIR/output_data" ]; then
  echo "ERROR: directory $JOBDIR does not contain output_data directory"
  exit 2
fi


cat << SCRIPT_EOF | qsub \
  -l walltime=4:00:00 \
  -l select=1:ncpus=1:mem=8gb \
  -o "$JOBDIR/plots-stdout.log" \
  -e "$JOBDIR/plots-stderr.log" \
  -v "JOBDIR=$JOBDIR" \
  -N "plt-$DESC_SUFFIX"

#!/bin/bash
set -Eeuxo pipefail
umask 007

module load python/3.6.8-gcc7.3.0
module load cairo

cd $JOBDIR
mkdir -p plots

###################
# all-clones stats
###################

python3 ~/SimBaD-analyzer/python/plot_stats.py \
	--input_file output_data/clone_stats_scalars.parquet \
	--time_file output_data/time_points.parquet \
	--output_prefix plots/ 

echo '
    birthEfficiency
    birthResistance
    lifespanEfficiency
    lifespanResistance
    successEfficiency
    successResistance' | \
	xargs -n 1 -I {} \
	python3 ~/SimBaD-analyzer/python/mullerplot-histogram-matplotlib.py \
		--param-name {} \
		--input-csv-file "output_data/histogram_{}.csv" \
		--time-parquet output_data/time_points.parquet/ \
		--stats-parquet output_data/clone_stats_scalars.parquet/ \
		--output-file "plots/histogram-{}"

###################
# noise stats
###################

python3 ~/SimBaD-analyzer/python/plot_stats.py \
	--input_file output_data/noise_stats_scalars.parquet \
	--time_file output_data/time_points.parquet \
	--output_prefix plots/noise-

#echo '
#    birthEfficiency
#    birthResistance
#    lifespanEfficiency
#    lifespanResistance
#    successEfficiency
#    successResistance' | \
#  xargs -n 1 -I {} \
#  python3 ~/SimBaD-analyzer/python/mullerplot-histogram-matplotlib.py \
#  	--param-name {} \
#  	--input-csv-file "output_data/noise_histogram_{}.csv" \
#    --time-parquet output_data/time_points.parquet/ \
#  	--stats-parquet output_data/noise_stats_scalars.parquet/ \
#  	--output-file "plots/noise-histogram-{}"


###################
# major-clones stats
###################

python3 ~/SimBaD-analyzer/python/plot_stats.py \
	--input_file output_data/major_stats_scalars.parquet \
	--time_file output_data/time_points.parquet \
	--output_prefix plots/major-

#echo '
#    birthEfficiency
#    birthResistance
#    lifespanEfficiency
#    lifespanResistance
#    successEfficiency
#    successResistance' | \
#  xargs -n 1 -I {} \
#  python3 ~/SimBaD-analyzer/python/mullerplot-histogram-matplotlib.py \
#  	--param-name {} \
#  	--input-csv-file "output_data/major_histogram_{}.csv" \
#    --time-parquet output_data/time_points.parquet/ \
#  	--stats-parquet output_data/major_stats_scalars.parquet/ \
#  	--output-file "plots/major-histogram-{}"


###################
# Muller plot
###################

python3 ~/SimBaD-analyzer/python/mullerplot-matplotlib.py \
	--input-file output_data/muller_data.parquet/ \
	--stats-file output_data/clone_stats_scalars.parquet/ \
	--params-file output_data/large_clones.parquet \
	--large-muller-order output_data/large_muller_order.parquet \
	--output-prefix plots/muller_plot_

####################
# Mutation histogram
####################

python3 ~/SimBaD-analyzer/python/mutation_histogram.py \
	--input_file output_data/large_final_mutations.parquet \
	--threshold 11 \
	--output_file plots/mutation_histogram.png

####################
# Mutation tree
####################

python3 ~/SimBaD-analyzer/python/mutation_tree_plot.py \
	--data-path output_data/large_final_mutations.parquet \
	--output-path plots/mutation-tree.png




touch PLOT_SUCCESS
SCRIPT_EOF


