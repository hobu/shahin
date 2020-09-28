# Import necessary packages
import os
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import rasterio as rio
import earthpy as et
import earthpy.plot as ep

import sys

import pathlib

p = pathlib.Path(sys.argv[1])

# Prettier plotting with seaborn
sns.set(font_scale=1.5, style="whitegrid")

# Open data and assign negative values to nan
with rio.open(str(p)) as src:
    dem = src.read(1, masked=True)

    std = np.std(dem)
    print (std)
#     fig, ax = plt.subplots(figsize = (10, 5))

#     im = ax.imshow(dem.squeeze())
#     ep.colorbar(im)
#     ax.set(title="Diff")
#     ax.set_axis_off()
#     plt.show()

# View object dimensions
    fig, ax = ep.hist(dem, colors=['grey'],
            title="3σ histogram %s" % p.name,
            xlabel='Band value (meters)',
            bins = 20,
            hist_range=(-3*std, 3*std))
    plt.text(0.5, 0.5, r'$n=%d,\ \sigma=%.3f$' % (len(dem), std), transform=ax.transAxes, bbox=dict(facecolor='red', ))
    plt.grid(True)
    plt.savefig('histogram-%s.png' % p.name, )
