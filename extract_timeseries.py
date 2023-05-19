import geopandas as gpd
import pandas as pd
import xarray as xr
from shapely.geometry import mapping
import os, sys, getopt, pathlib, logging
import requests, pycountry

def main(argv) -> None:
    """
    Function used to check the commandline arguments and launch the extraction.
    """
    if len(argv) != 10:
        logging.error("Missing arguments:")
        logging.error("python extract_timeseries.py -i <inputfile> -o <outputfile> -v <varname> -c <country_iso3> -a <adm_level>")
        sys.exit()

    # check arguments
    opts, _ = getopt.getopt(argv,"hi:o:v:a:c:",["ifile=","ofile=", "varname=", "adm_level=", "country_iso3="])
    for opt, arg in opts:
        if opt == '-h':
            print ('python extract_timeseries.py -i <inputfile> -o <outputfile> -v <varname> -c <country_iso3> -a <adm_level>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg
        elif opt in ("-v", "--varname"):
            varname = arg
        elif opt in ("-a", "--adm_level"):
            adm_level = arg
        elif opt in ("-c", "--country_iso3"):         
            country = arg
    # Launch the main function
    extract_time_series(inputfile, outputfile, varname, adm_level, country)

def extract_time_series(inputfile:str, outputfile:str, varname:str, adm_level:str, country:str) -> None:
    """
    Extract the time-series from a GRIB file (tested and designed for ERA5-land)

    Parameters
    ----------
    inputfile : str
        Path to the GRIB file to use
    outputfile : str
        Path of the parquet file where time-series will be stored
    varname : str
        Data variable contained in the input file
    adm_level : str
        Administrative level to use for the target time-series (allowed values: 0, 1, 2)
    country : str
        Country ISO3 code
    """
    # Load the GRIB file using xarray ------------------------------------------
    if os.path.exists(inputfile):
        dataset = xr.open_dataset(inputfile)
    else:
        logging.error(f"Missing input file ({inputfile})")
        raise Exception(f"Missing input file ({inputfile})")
    
    # Checking if the varname is in the xarray inputfile -----------------------
    if varname not in dataset.data_vars:
        logging.error(f"Variable {varname} is not a data variable in the target GRIB file. Available variables: {dataset.data_vars}")
        raise Exception(f"Variable {varname} is not a data variable in the target GRIB file. Available variables: {dataset.data_vars}")


    # check if country is in all_countries -------------------------------------
    country = country.upper()
    all_countries = [x.alpha_3 for x in list(pycountry.countries)]
    if not country in all_countries:
        logging.error(f"country {country} ISO3 code not existing")
        raise Exception(f"country {country} ISO3 code not existing")

    # Output file --------------------------------------------------------------
    if pathlib.Path(outputfile).suffix != ".parquet":
        logging.warning("Adding the parquet extension to the output file")
        target_file_name = f"{outputfile}.parquet"
    else:
        target_file_name = outputfile
    
    try:
        r = requests.get(f"https://www.geoboundaries.org/api/current/gbOpen/{country}/ALL/").json()
    except: 
        logging.error('Error when requesting the shapefile')

    # check adm_level range -----------------------------------------------------
    adm_level = int(adm_level)
    if adm_level < 0 or adm_level > 2:
        logging.error("Adm_level must be a value between 0 and 2")
        raise Exception("Adm_level must be a value between 0 and 2")
    
    if len(r) > 2:
        selected_adm = r[adm_level]
    else:
        logging.warning(f"Adm level {adm_level} for {country} does not exist: switching to ADM0")
        selected_adm = r[0]
    
    # Selecting the simplified GeoJSON from the returned data
    dlPath = selected_adm["simplifiedGeometryGeoJSON"]
    geoBoundary = gpd.read_file(dlPath)

    out_ts = pd.DataFrame() # Time-series
    stats  = pd.DataFrame() # statistics

    for index, row in geoBoundary.iterrows():
        # oneliner from https://stackoverflow.com/questions/3160699/python-progress-bar
        print(end="\r|%-80s|" % ("="*(80*(index+1)//geoBoundary.shape[0] )))

        polygon_gdf = geoBoundary.iloc[[index]]
        xmin, ymin, xmax, ymax = polygon_gdf.total_bounds

        this_dataset = dataset.sel(
            longitude = slice(xmin, xmax), 
            latitude = slice(ymax, ymin))
        # Saving statistics
        stats = pd.concat(
            [stats, pd.DataFrame(
            {
                'name': [row["shapeName"]],
                'ISO': [row["shapeISO"]],
                'area_km2': round(polygon_gdf.to_crs({'proj': 'cea'}).area/1e6, 2),
                'n': len(this_dataset['latitude']) * len(this_dataset['longitude'])
            })
            ]
        )
        
        if this_dataset[varname].size > 0:
            sel = this_dataset.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
            sel.rio.write_crs("epsg:4326", inplace=True)

            clipped = (sel
                       .rio
                       .clip(
                        polygon_gdf.geometry.apply(mapping), 
                        polygon_gdf.crs, drop=False
                        )
            )

            stacked_data = (clipped
                            .mean(dim = ['longitude', "latitude"])
                            .stack(dimension = ['time', 'step'])
            )

            stacked_data['timestep'] = stacked_data.time + stacked_data.step
            # Adding the timestep column at the first iteration
            if len(out_ts) == 0:
                out_ts['timestep'] = stacked_data['timestep'].values
            
            out_ts[ row["shapeName"] ] = stacked_data[varname].values

            
    # Saving files
    out_ts.to_parquet(target_file_name)
    stats.to_csv(f'{target_file_name}.csv')



if __name__ == "__main__":
   main(sys.argv[1:])