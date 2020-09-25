import base64
import boto3
import os
from urllib.parse import urlparse
import sys
import io
import tempfile
import subprocess
import json

import logging
logger = logging.getLogger('dirunal')

ch = logging.StreamHandler(stream=sys.stderr)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

TEMPDIR = '/data'


def get_centroid(args):

    logger.debug(args.boundary)
    pipeline = """[
    {
        "type": "readers.las",
        "filename": "/data/fixed.las",
        "tag":"fixed"
    },
    {
        "type": "filters.stats"
    }

]""" % args.__dict__

    pipeline = json.loads(pipeline)
    pipeline = json.dumps(pipeline)
    logger.debug(pipeline)

    rargs = ['pdal', 'pipeline', '--stdin', '--debug', '--metadata', 'STDOUT']
    results = run(rargs, pipeline, return_json=True)
    box = results['stages']['filters.stats']['bbox']['native']['bbox']
    def avg(items):
        return sum(items) / len(items)
    x = avg([box['minx'], box['maxx']])
    y = avg([box['miny'], box['maxy']])
    z = avg([box['minz'], box['maxz']])
    centroid = f'{x:.4f} {y:.4f} {z:.4f}'
    args.centroid = centroid



def apply_transform_icp(args):
    import numpy as np

    transform = args.transform['transform']
    centroid = args.transform['centroid']
#    centroid = args.centroid

    transform = np.array([float(i) for i in transform.split()])
    x, y, z = [float(t) for t in centroid.split()]
    coords = np.array([x, y, z])
    identity = np.identity(4, dtype=np.double)
    coords = coords

    identity[0][3] = -1 * coords[0]
    identity[1][3] = -1 * coords[1]
    identity[2][3] = -1 * coords[2]
    center = identity

    center_filter = {
        'type': 'filters.transformation',
        'tag': 'center',
        'matrix': ' '.join(['%E' % i for i in center.ravel().tolist()])
    }
    transform_filter = {
        'type': 'filters.transformation',
        'tag': 'transform',
        'matrix': ' '.join(['%E' % i for i in transform.ravel().tolist()])
    }

    uncenter = np.identity(4, dtype=np.double)
    uncenter[0][3] = coords[0]
    uncenter[1][3] = coords[1]
    uncenter[2][3] = coords[2]
    uncenter_filter = {
        'type': 'filters.transformation',
        'tag': 'decenter',
        'matrix': ' '.join(['%E' % i for i in uncenter.ravel().tolist()])
    }
    args.center_filter = json.dumps(center_filter)
    args.transform_filter = json.dumps(transform_filter)
    args.uncenter_filter = json.dumps(uncenter_filter)

def apply_transform_cpd(args):
    import numpy as np

    transform = args.transform['transform']

    transform = np.array([float(i) for i in transform.split()])
    transform_filter = {
        'type': 'filters.transformation',
        'tag': 'transform',
        'matrix': ' '.join(['%E' % i for i in transform.ravel().tolist()])
    }
    args.transform_filter = json.dumps(transform_filter)


def run(args, stdin=None, return_json=False):
    logger.debug(stdin)
#    dargs = ['docker','run','-a','stdin','-a','stdout','-a','stderr','-v','`pwd`:/data','-e','TMPDIR=/data','-i','crrel-conda']
    args = args
    logger.debug(' '.join(args))
    p = subprocess.Popen(' '.join(args),
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True,
                         encoding='utf8')
    ret = p.communicate(input=stdin)
    if p.returncode != 0:
        error = ret[1]
        logger.debug(error)
        sys.exit(0)
        return False

    if return_json:
        logger.debug(ret[0])
        return json.loads(ret[0])
    return True


def compute_transform(args):

    logger.debug(args.boundary)
    pipeline = """[
    {
        "type": "readers.las",
        "filename": "/data/fixed.las",
        "tag":"fixed"
    },
    {
        "type": "readers.ept",
        "filename": "%(url_float)s" ,
        "polygon":"%(boundary)s",
        "tag":"float_ept"
    },

    {
        "inputs":["float_ept"],
        "type": "filters.range",
        "limits": "Intensity[1:]",
        "tag":"float_range"
    },
    {
        "inputs":["float_range"],
        "type": "filters.sample",
        "radius": 5.0,
        "tag":"float_sample"
    },
    {
        "inputs":["float_sample"],
        "type": "filters.outlier",
        "tag":"float_outlier"
    },
    {
        "inputs":["fixed", "float_outlier"],
        "type": "filters.icp",
        "tag":"icp"
    },
    {
        "inputs":["icp"],
        "tag":"writer",
        "type": "writers.las",
        "filename": "/data/sampled.laz"
    }
]""" % args.__dict__

    f = open('compute-adjustment.json','wb')
    f.write(pipeline.encode('utf-8'))
    f.close()
    pipeline = json.loads(pipeline)
    pipeline = json.dumps(pipeline)
    logger.debug(pipeline)

    rargs = ['pdal', 'pipeline', '--stdin', '--debug', '--metadata', 'STDOUT']
    results = run(rargs, pipeline, return_json=True)
    #args.transform = results['stages']['filters.cpd']
    args.transform = results['stages']['filters.icp']
    args.vlr = base64.b64encode(str(args.transform).encode('utf-8')).decode('utf-8')
    logger.debug(args.transform)


def adjust_floating(args):

    apply_transform_icp(args)

    pipeline = """[

        {
            "type": "readers.ept",
            "filename": "%(url_float)s" ,
            "threads":16
       },
       %(center_filter)s,
       %(transform_filter)s,
       %(uncenter_filter)s,
        {
            "type":"writers.las",
            "scale_x":"0.001",
            "scale_y":"0.001",
            "scale_z":"0.001",
            "offset_x":"auto",
            "offset_y":"auto",
            "offset_z":"auto",
            "pdal_metadata":"true",
            "filename":"%(adjusted)s",
            "vlrs": [{
            "description": "Diurnal adjustment",
            "record_id": 666,
            "user_id": "crrel",
            "data": "%(vlr)s"
            }]

        }
    ]""" % args.__dict__

    f = open('adjust-floating.json','wb')
    f.write(pipeline.encode('utf-8'))
    f.close()
    logger.debug(pipeline)
    pipeline = json.loads(pipeline)
    pipeline = json.dumps(pipeline)
    logger.debug(pipeline)

    args = ['pdal', 'pipeline', '--stdin', '--debug', '--metadata', 'STDOUT']
    results = run(args, pipeline, return_json=True)
    return results


def dump(args):

    pipeline = """[

        {
            "type": "readers.ept",
            "filename": "%(url_float)s" ,
            "threads":16
       },
        {
            "type":"writers.gdal",
            "bounds":"([530373, 542937], [7354064, 7366504])",
            "resolution":"2.0",
            "data_type":"float",
            "filename":"float.tif"
        }
    ]""" % args.__dict__

    pipeline = json.loads(pipeline)
    pipeline = json.dumps(pipeline)

    rargs = ['pdal', 'pipeline', '--stdin', '--debug', '--metadata', 'STDOUT']
    results = run(rargs, pipeline, return_json=True)

    pipeline = """[

        {
            "type": "readers.las",
            "filename": "%(adjusted)s"
       },
        {
            "type":"writers.gdal",
            "bounds":"([530373, 542937], [7354064, 7366504])",
            "resolution":"2.0",
            "data_type":"float",
            "filename":"adjusted.tif"
        }
    ]""" % args.__dict__



#         {
#             "type":"writers.las",
#             "scale_x":"0.001",
#             "scale_y":"0.001",
#             "scale_z":"0.001",
#             "offset_x":"auto",
#             "offset_y":"auto",
#             "offset_z":"auto",
#             "pdal_metadata":"true",
#             "filename":"adjusted.las"
#
#         }
    pipeline = json.loads(pipeline)
    pipeline = json.dumps(pipeline)

    results = run(rargs, pipeline, return_json=True)

    rargs = ["gdal_calc.py", "-A", "adjusted.tif",
                             "-B", "float.tif",
                             "--outfile=diff-float.tif",
                             "--calc=\"A-B\"",
                             "--A_band=4",
                             "--overwrite",
                             "--B_band=4"]
    results = run(rargs, pipeline, return_json=False)


    rargs = ["gdal_calc.py", "-A", "fixed.tif",
                             "-B", "float.tif",
                             "--outfile=diff-fixed.tif",
                             "--calc=\"A-B\"",
                             "--A_band=4",
                             "--overwrite",
                             "--B_band=4"]
    results = run(rargs, pipeline, return_json=False)

    rargs = ["python", "hist.py", "diff-float.tif"]
    results = run(rargs, pipeline, return_json=False)

    rargs = ["python", "hist.py", "diff-fixed.tif"]
    results = run(rargs, pipeline, return_json=False)
    return results

def entwine(args):
    bounds = '[528811,7317465,-100,542056,7363734,500]'
    args = ['entwine', 'build', '-i', args.adjusted,
            '-o', args.ept, '--bounds', bounds]
    results = run(args)
    return results


def upload(args):
    cargs = ['aws', 's3', 'sync', args.ept,
            's3://grid-public-ept/atlas/diurnal/'+args.outpath +'/'+args.scan_name,
            '--acl', 'public-read']
    logger.debug(' '.join(cargs))

    cargs = ['aws', 's3', 'cp', args.adjusted,
            's3://grid-glacierscans/diurnal/'+args.outpath + '/' +args.scan_name +'.laz']
    logger.debug(' '.join(cargs))
    #run(args)

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Difference two EPT datasets')
    parser.add_argument('url_float', type=str,
                        help='EPT URL for floating data')
    parser.add_argument('--boundary', type=str,
                        default="""POLYGON ((535873.126869312 7357845.72366887,535648.541547531 7357810.92875986,535683.33645654 7357709.70720638,536221.075959396 7357390.22667821,536379.234636707 7357548.38535552,536451.98762827 7357741.33894184,536382.397810253 7357864.70271014,536224.239132942 7357896.33444561,536081.896323363 7357883.68175142,535873.126869312 7357845.72366887)) / EPSG:32624""",
                        help='Boundary geometry')

    args = parser.parse_args()

# url = 'https://grid-public-ept.s3.amazonaws.com/atlas/ATLAS-South/2015/150911_000218/ept.json'
    args.scan_name = args.url_float.split('/')[-2]
    args.year = args.url_float.split('/')[-3]
    args.scanner = args.url_float.split('/')[-4].split('-')[1]
    args.outpath = f'ATLAS-{args.scanner}/{args.year}'
    args.TEMPDIR = TEMPDIR
    args.adjusted = f'{args.TEMPDIR}/{args.scan_name}.laz'
    args.ept = f'{args.TEMPDIR}/ept/{args.scan_name}'

#    get_centroid(args)
    compute_transform(args)
    adjust_floating(args)
    dump(args)
    entwine(args)
#    upload(args)


if __name__ == "__main__":
    main()
