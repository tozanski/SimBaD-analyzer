#! /bin/bash
set -euxo pipefail

rm -f ./SimBaD/build/compile.output ./SimBaD/build/compile.error

if [ $# -eq 0 ]; then
  SIMBAD_DIR="$HOME/SimBaD"
elif [ $# -eq 1 ]; then
  SIMBAD_DIR=`readlink -f $1`
else
  echo "illegal number of paramters"
fi 

if [ ! -d "$SIMBAD_DIR" ]; then
  echo "$SIMBAD_DIR" does not exist >&2
  exit 1
fi

cat << EOF | qsub \
  -l walltime=00:15:00 \
  -l select=1:ncpus=4:mem=8gb \
  -o "$SIMBAD_DIR/build/compile.output" \
  -e "$SIMBAD_DIR/build/compile.error" \
  -N compile-$SIMBAD_DIR \


#! /bin/bash
set -euxo pipefail

module load boost/1.68.0-gcc6.2.0
module load cmake/3.10.2

mkdir -p "$SIMBAD_DIR/build" && cd "$SIMBAD_DIR/build"
cmake ../src \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=gcc \
  -DCMAKE_CXX_COMPILER=g++ \
  -DBoost_NO_SYSTEM_PATHS=ON \
  -DBOOST_ROOT=/usr/local/boost/gcc-6.2.0/1.62.0/ \
  -DBOOST_LIBRARY_DIR=/usr/local/boost/gcc-6.2.0/1.68.0/lib/libboost_program_options-gcc62-mt-x64-1_68.so \


make -j4

ctest --VV -output-on-failure
EOF

