# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a research project validating whether NASA's SWOT (Surface Water and Ocean Topography) satellite has measurement issues when it is raining while measurements are being taken. It pairs SWOT river node data with USGS stream gauge observations and MRMS radar-based precipitation data, calculating water surface elevation error during rainfall events.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate swot
```

Additional packages not in environment.yml that are used in notebooks:
- `dataretrieval` — USGS water data API (`pip install dataretrieval`)
- `tqdm` — progress bars

## Repository Structure

```
notebooks/       # Jupyter notebooks (primary work) + Python utility modules
data/            # Local data files (parquet, CSV, GeoPackage)
figures/         # Output plots
```

### Key utility modules in `notebooks/`

- **`download.py`** — Data retrieval functions:
  - `fetch_hydrocron` / `fetch_hydrocron_node` / `fetch_hydrocron_pass` — pull SWOT time series from the PO.DAAC Hydrocron API
  - `download_mrms` / `download_mrms_vectorized` — download MRMS precipitation data from NOAA S3 and co-locate it to SWOT nodes
- **`utilities.py`** — Helper functions:
  - `filter_bits(bitmask)` — checks SWOT quality bitmask bits 13/14 (rain flags)
  - `retrieve_stations(data_dir, maximum_match_distance)` — loads SWOT–USGS station pairs from parquet, filters by distance and 8-digit site IDs
  - `retrieve_active_stations` / `retrieve_active_station_data` — queries USGS waterdata API for stage (parameter `00065`) time series
- **`plot.py`** — `plot_heatmap()` for node × time heatmaps

### Data sources

| Source | Access method | Notes |
|--------|--------------|-------|
| SWOT river nodes | Hydrocron REST API (PO.DAAC) | Collection `SWOT_L2_HR_RiverSP_D` |
| USGS stream gauge stage | `dataretrieval.waterdata` | Parameter code `00065` (gage height) |
| MRMS precipitation | NOAA public S3 bucket | GRIB2 format, 2-minute or hourly intervals |
| SWOT–USGS node pairings | `data/dfms_v1_us_station_matches.parquet` | Pre-computed match distances in meters |

## Key Data Concepts

- **SWOT node IDs** are used as the primary join key between SWOT and USGS data. Station matches are filtered to `match_dist_m < 200` m and 8-digit USGS site numbers.
- **SWOT bitmask bits 13 and 14** are rain/ice flags. `filter_bits()` returns `True` when neither bit is set (clean observation).
- **MRMS longitude** is stored 0–360°; convert with `lon % 360` when interpolating against SWOT coordinates.
- **Hydrocron API** returns CSV embedded in JSON; parse with `pd.read_csv(StringIO(...), na_values=['no_data'])`.
- USGS data is batched in groups of 50 stations to stay within API limits.
