import gzip
import os
import pathlib
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO, BytesIO

import earthaccess
import geopandas as gpd
import holoviews as hv
import hvplot.pandas
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shapely
import requests
import rioxarray as rxr
import xarray as xr
from tqdm.notebook import tqdm

from download import fetch_hydrocron, download_mrms
from plot import plot_heatmap
from utilities import filter_bits

# Preferences
use_cache = True

# Set up data path
data_dir = pathlib.Path().cwd().parent / 'data'
data_dir.mkdir(exist_ok=True, parents=True)
precip_path = data_dir / 'MRMS_{var_name}.csv'
swot_path = data_dir / 'swot_timeseries_new.csv'

# Satellite tracks for test site on Willamette River
passes = [39, 274, 345]
# Site bounding box
bounds = (-123.4, 44, -123, 45)
bbox = shapely.geometry.box(*bounds)
start_time = '2023-01-01T00:00:00Z'
end_time = '2026-01-01T00:00:00Z'


if __name__ == "__main__":
    # Download MRMS data
    earthaccess.login()

    swot_ts_df = (
        pd.read_csv(
            swot_path, 
            na_values=['no_data'],
            index_col=['node_id', 'time_str'],
            parse_dates=True)
        .rename_axis(index={"time_str": "time"})
    )
    # Remove NA rows
    swot_ts_df = swot_ts_df[swot_ts_df.index.get_level_values("time").notna()]

    # MRMS variable names
    mrms_vars = dict(
        flag=('PrecipFlag_00.00', True),  # Binary flag (0/1) for precipitation
        rate=('PrecipRate_00.00', False),  # Precipitation rate (mm/hr)
        quality=('RadarAccumulationQualityIndex_01H_00.00', True)  # Quality index (0-100)
        #zdr=('MergedZdr_00.00', False)  # Differential reflectivity (dB)
    )
    # Download MRMS data for each variable if not already downloaded
    for _, (var_name, hourly) in mrms_vars.items():
        var_path = precip_path.parent / precip_path.name.format(var_name=var_name)
        print(var_path)
        if not var_path.exists() or not use_cache:
            download_mrms(
                swot_ts_df, 
                var_name=var_name, 
                precip_path=precip_path,
                hourly=hourly)