# Filter data
import math
import requests
from io import StringIO

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from IPython.display import Image, display
import os
import pathlib
from pathlib import Path
import dataretrieval.waterdata as waterdata
from datetime import datetime, timedelta, timezone

def filter_bits(bitmask, bits_to_check=[13, 14]):
    """
    Filter bitmask to check if bits are set.
    
    Parameters
    ----------
    bitmask : int
        Integer bitmask to check.
    bits_to_check : list of int, optional
        List of bit positions to check (0-indexed). Default is [13, 14].
    """
    flagged = []
    for bit in bits_to_check:
        flagged.append((bitmask & 2**bit) > 0)
    return not any(flagged)


def retrieve_stations(data_dir=Path('/Users/masa6503/repos/swot-precip-validation/data'), file_name="dfms_v1_us_station_matches.parquet", maximum_match_distance=200):
    stations = pd.read_parquet(data_dir / file_name)
    stations = stations.query(f"match_dist_m<{maximum_match_distance}")
    stations = stations[stations['usgs_site_no'].astype(str).str.len() == 8]
    return stations


def retrieve_active_stations(usgs_id_list, timeseries_start="2023-04-01T00:00:00Z"):
    tomorrow = datetime.now(tz=timezone.utc) + timedelta(days=2)
    # Format as ISO 8601 UTC midnight
    tomorrow_str = tomorrow.strftime("%Y-%m-%dT00:00:00Z")
    timeseries = timeseries_start+"/"+tomorrow_str
    all_active_ids = []
    for i in range(0, len(usgs_id_list), 50):
        end_index = min(i + 50, len(usgs_id_list))
        # print(i, end_index)
        active_ids = list(waterdata.get_latest_continuous(
            monitoring_location_id=list(usgs_id_list[i:end_index]), 
            parameter_code="00065", 
            time=timeseries
        )[0]['monitoring_location_id'].unique())
        all_active_ids.extend(active_ids)
    return (all_active_ids)


def retrieve_active_station_data(active_station_list, timeseries="2023-04-01T00:00:00Z/2026-02-01T12:31:12Z", 
                                 output_dir = '/Users/masa6503/repos/swot-precip-validation/data/usgs_stage_data'):
    filenames=[]
    for i in range(0, len(active_station_list), 50):
        end_index = min(i + 50, len(active_station_list))
        filename_to_save=f'{output_dir}/usgs_stage_data_{i}_{end_index}.parquet'
        if os.path.exists(filename_to_save):
            print(f"File {filename_to_save} already exists. Skipping retrieval.")
            filenames.append(filename_to_save)
            continue
        print(i, end_index)
        try:
            data = waterdata.get_continuous(
                monitoring_location_id=list(active_station_list[i:end_index]), 
                parameter_code="00065", 
                time=timeseries
            )[0]
            data.to_parquet(filename_to_save)
            filenames.extend(filename_to_save)
        except Exception as e:
            print(f"Error retrieving data for stations {active_station_list[i:end_index]}: {e}")
    return filenames