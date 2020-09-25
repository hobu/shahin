#!/bin/bash

boundary="POLYGON ((535975.289132994 7357844.7009714,536208.243343056 7357913.05607998,536462.387636779 7357881.3393096,536512.696996698 7357689.94500556,536262.243878839 7357606.82519352,535975.289132994 7357844.7009714)) / EPSG:32624"

ept="https://grid-public-ept.s3.amazonaws.com/atlas/ATLAS-South/2018/180806_190203/ept.json"

# ./fixed.sh float.las https://grid-public-ept.s3.amazonaws.com/atlas/ATLAS-South/2018/180809_190159/ept.json
output=$1
ept=$2
command="pdal pipeline fixed.json --readers.ept.filename=$ept --writers.las.filename=$output --readers.ept.polygon=\"$boundary\" \
    --writers.las.scale_x=\"0.001\" \
    --writers.las.scale_y=\"0.001\" \
    --writers.las.scale_z=\"0.001\" \
    --writers.las.offset_x=\"auto\" \
    --writers.las.offset_y=\"auto\" \
    --writers.las.offset_z=\"auto\" \
    --filters.sample.radius=5.0 \
    sample"

echo $command


eval $command
