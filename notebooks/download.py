import gzip
import tempfile
from io import BytesIO, StringIO

import pandas as pd
import requests
import xarray as xr
from tqdm.notebook import tqdm
import time


def fetch_hydrocron(info, start_time, end_time, fields, collection_type = "SWOT_L2_HR_RiverSP_D"):
    """
    Fetch hydrocron data for a given node.
    
    Parameters
    ----------
    info : dict
        Dictionary with keys:
        - 'node_id': str, node identifier
        - 'start_time': str, start time in ISO format
        - 'end_time': str, end time in ISO format
        - 'fields': list of str, fields to retrieve (e.g., ['wse', 'width'])
    """

    hydrocron_url_template = (
        "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
        "?feature=Node&feature_id={node_id}"
        "&start_time={start_time}&end_time={end_time}"
        "&output=csv"
        "&collection_name={collection_type}"
        "&fields={fields},"
    )
    
    node_id = info['node_id']
    try:
        response = requests.get(
            hydrocron_url_template.format(
                node_id=node_id, fields=','.join(fields),
                start_time=start_time, end_time=end_time, collection_type=collection_type),
            timeout=10  # Avoid hanging requests
        )
        if response.status_code != 200:
            print(f"Error fetching hydrocron data for "
                  f"{node_id}: {response.json().get('error')}")
            return None

        hydrocron_df = pd.read_csv(
            StringIO(response.json()['results']['csv']),
            index_col=['node_id', 'time_str'],
            parse_dates=True,
            na_values=['no_data']
        )
        return hydrocron_df

    except Exception as e:
        print(f"Exception for node {node_id}: {e}")
        return None

def fetch_hydrocron_node(node_id, start_time, end_time, fields, collection_type = "SWOT_L2_HR_RiverSP_D"):
    """
    Fetch hydrocron data for a given node.
    
    Parameters
    ----------
    info : dict
        Dictionary with keys:
        - 'node_id': str, node identifier
        - 'start_time': str, start time in ISO format
        - 'end_time': str, end time in ISO format
        - 'fields': list of str, fields to retrieve (e.g., ['wse', 'width'])
    """

    hydrocron_url_template = (
        "https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries"
        "?feature=Node&feature_id={node_id}"
        "&start_time={start_time}&end_time={end_time}"
        "&output=csv"
        "&collection_name={collection_type}"
        "&fields={fields},"
    )
    
    # node_id = info['node_id']
    try:
        response = requests.get(
            hydrocron_url_template.format(
                node_id=node_id, fields=','.join(fields),
                start_time=start_time, end_time=end_time, collection_type=collection_type),
            timeout=10  # Avoid hanging requests
        )
        if response.status_code != 200:
            print(f"Error fetching hydrocron data for "
                  f"{node_id}: {response.json().get('error')}")
            return None

        hydrocron_df = pd.read_csv(
            StringIO(response.json()['results']['csv']),
            index_col=['node_id', 'time_str'],
            parse_dates=True,
            na_values=['no_data']
        )
        return hydrocron_df

    except Exception as e:
        print(f"Exception for node {node_id}: {e}")
        return None
    

def download_mrms(ts_df, var_name, precip_path, hourly=False):
    """
    Download MRMS data to match SWOT time series data.
    
    Parameters
    ----------
    ts_df : pd.DataFrame
        DataFrame with columns ['node_id', 'lat', 'lon', 'time_str']
    var_name : str
        MRMS variable name, e.g., 'PrecipRate'
    hourly : bool, optional
        If True, download hourly data (minute=0), else every 2 minutes.
        Default is False.
    """
    new_rows = []
    for dt, swot_df in tqdm(ts_df.reset_index().groupby('time')):
        ## Build MRMS URL
        # Convert to UTC just in case
        dt_utm0 = dt.tz_convert('UTC')
        # Get date components
        year, month, day = dt_utm0.year, dt_utm0.month, dt_utm0.day
        hour, minute = dt_utm0.hour, dt_utm0.minute
        # Round down to nearest even minute (MRMS data is every 2 minutes)
        if hourly:
            mrms_minute = 0  # Use the hour only
        else:
            # Round down to nearest even minute
            mrms_minute = (minute // 2) * 2
        # Format MRMS URL
        mrms_url = (
            "https://noaa-mrms-pds.s3.amazonaws.com/CONUS"
            f"/{var_name}"
            f"/{year}{month:02d}{day:02d}"
            f"/MRMS_{var_name}"
            f"_{year}{month:02d}{day:02d}-{hour:02d}{mrms_minute:02d}00"
            ".grib2.gz"
        )
        print(mrms_url)
        # Download the MRMS data
        start_time = time.time()
        response = requests.get(mrms_url)
        if response.status_code != 200:
            print(f"Failed to download MRMS data for {dt_utm0}: "
                f"{response.status_code}")
            continue

        # Read the GRIB2 data from the gzipped content
        with gzip.open(BytesIO(response.content), 'rb') as gz:
            grib_bytes = gz.read()

        # Write to a temporary .grib2 file
        with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
            tmp.write(grib_bytes)
            tmp.flush()  # Ensure all bytes are written

            # Read it with xarray + cfgrib
            ds = xr.open_dataset(tmp.name, engine='cfgrib')

            end_time = time.time()
            print(f"Downloaded MRMS data for {dt_utm0} in {end_time - start_time:.2f} seconds")
            for _, row in swot_df.iterrows():
                # Get the nearest MRMS data for the SWOT nodes
                ds_var_name = list(ds.data_vars)[0]  # MRMS file has one variable
                var_value = ds[ds_var_name].interp(
                    latitude=row['lat'],
                    longitude=row['lon'] % 360, # Longitude is 0-360
                    method='nearest'
                ).values
                new_rows.append(pd.DataFrame({
                    'node_id': row['node_id'],
                    'time': dt,
                    var_name: var_value
                }, index=[0]))
            loop_time = time.time() - end_time
            print(f"Processed {len(swot_df)} nodes for {dt_utm0} in {loop_time:.2f} seconds")
            

    precip_df = pd.concat(new_rows)
    precip_df.to_csv(
        precip_path.parent / precip_path.name.format(var_name=var_name), 
        index=False)