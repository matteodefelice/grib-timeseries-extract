# Extract time-series from ERA5-land using political administrative boundaries

This Python code can extract temporal time-series from a [ERA5-land GRIB file](https://cds.climate.copernicus.eu/cdsapp#!/dataset/reanalysis-era5-land) using political administrative boundaries (from ADM0 to ADM2) for any country.

The political boundaries are obtained from [geoBoundaries](https://www.geoboundaries.org/) via their [API](https://www.geoboundaries.org/api.html). 

The code extract the time-series in a Parquet file, for a column for each administrative region. It also generates a file with information regarding the selected areas including the number of grid points for each region and its area in squared kms. 

# Requirements
The Python requirements are in the file `requirements.txt`.

# Usage
The Python code can be used by the command line. 
```
python extract_timeseries.py -i <inputfile> -o <outputfile> -v <varname> -c <country_iso3> -a <adm_level>
```

For example, if I want to extract the time-series for the Netherlands provinces (ADM1) for temperature data downloaded for a specific month:
```
python extract_timeseries.py -i data\era5land-europe-t2m-202303.grib -o nld-t2m-202303.parquet -v t2m -c NLD -a 1
```

At the end of the execution, two files will be generated:
  1. ` nld-t2m-202303.parquet` with the time-series data
  2. ` nld-t2m-202303.parquet.csv` with the information about the time-series

# Notes and limitations
- Unfortunately, Dask does not work well with GRIB files (see [here](https://github.com/ecmwf/cfgrib/issues/311)) and then the GRIB file must be loaded in memory. For this reason, I would suggest using ERA5-land hourly data not longer than 1-month and on a small domain (not global)
- The code is pretty simple and it can be extended to work with other datasets and other formats (for example NetCDF)
- The code uses the geoBoundaries APIs to retrieve the administrative boundaries, however it can be easily extended to read the boundaries from a downloaded file ([example](https://www.geoboundaries.org/downloadCGAZ.html))