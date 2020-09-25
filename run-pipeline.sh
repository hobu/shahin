#!/bin/bash


rm -rf ept/
mkdir ept/
infile="$1"

read -r -d '' pipeline << EOM
[
    {
        "type": "readers.ept",
        "filename": "https://grid-public-ept.s3.amazonaws.com/atlas/ATLAS-South/2018/180809_190159/ept.json",
        "threads": 16
    },
    {
        "type": "filters.range",
        "limits":"Intensity[1:]"
    },
    {
        "type": "filters.sample",
        "radius":1.0
    },
        {
            "type":"writers.gdal",
            "bounds":"([530373, 542937], [7354064, 7366504])",
            "resolution":"2.0",
            "data_type":"float",
            "filename":"fixed.tif"
        }
]
EOM


echo $pipeline | pdal pipeline --stdin

