import logging
import subprocess
import os
import json

def dataset_has_depth(dataset_id):
    result = subprocess.run(
        ["copernicusmarine", "describe", "--dataset-id", dataset_id],
        capture_output=True, text=True, check=True
    )
    metadata = json.loads(result.stdout)
    dimensions = metadata.get('dimensions', {})
    return 'depth' in dimensions

def download_copernicus(boundary_box, year):
    """
    Downloads Copernicus Marine data using the Copernicus Marine API for August to October of a given year.
    
    Args:
        boundary_box (dict): Dictionary containing the boundary box
        year (int): Year for which data is to be downloaded
        dataset_ids (list): List of dataset IDs
        output_directory (str): Directory to save the downloaded data
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    output_directory = 'data'

    if year == 2021:
        dataset_ids = ['cmems_mod_glo_phy_myint_0.083deg_P1D-m','cmems_mod_glo_bgc_my_0.25deg_P1D-m']
    else:
        dataset_ids = ['cmems_mod_glo_phy_my_0.083deg_P1D-m', 'cmems_mod_glo_bgc_my_0.25deg_P1D-m']

    lat_min = boundary_box['lat_min']
    lat_max = boundary_box['lat_max']
    lon_min = boundary_box['lon_min']
    lon_max = boundary_box['lon_max']

    start_datetime = f"{year}-08-01T00:00:00"
    end_datetime = f"{year}-10-31T23:59:59"

    logging.info(f"Starting Copernicus data download for {year}")
    
    # Ensure credentials are available
    if 'CMEMS_USERNAME' not in os.environ or 'CMEMS_PASSWORD' not in os.environ:
        logging.error("Copernicus Marine credentials not found in environment variables")
        return False
    
    no_depth_datasets = ['cmems_mod_glo_phy_anfc_0.083deg_P1D-m', 'cmems_mod_glo_phy_my_0.083deg_P1D-m','cmems_mod_glo_phy_myint_0.083deg_P1D-m','cmems_mod_glo_bgc_my_0.25deg_P1D-m']

    # Loop through each dataset and download
    for dataset_id in dataset_ids:
        logging.info(f"Downloading dataset: {dataset_id}")
        
        dataset_output_dir = os.path.join(output_directory, dataset_id)
        os.makedirs(dataset_output_dir, exist_ok=True)
        

        try:
            cmd = [
                "copernicusmarine", "subset",
                f"--dataset-id={dataset_id}",
                f"--minimum-longitude", str(lon_min),
                f"--maximum-longitude", str(lon_max),
                f"--minimum-latitude", str(lat_min),
                f"--maximum-latitude", str(lat_max),
                "--file-format", "netcdf",
                f"--output-directory", dataset_output_dir,
                f"--start-datetime", start_datetime,
                f"--end-datetime", end_datetime,
                "--overwrite",
                "--username", os.environ['CMEMS_USERNAME'],
                "--password", os.environ['CMEMS_PASSWORD'],
            ]

            if dataset_id == 'cmems_mod_glo_bgc_my_0.25deg_P1D-m':
                cmd += ["--minimum-depth", "0.5057600140571594", "--maximum-depth", "0.5057600140571594"]
            else:
                cmd += ["--minimum-depth", "0.49402499198913574", "--maximum-depth", "0.49402499198913574"]

                    
            subprocess.run(cmd, check=True)
            logging.info(f"Successfully downloaded dataset: {dataset_id}")
            
        except subprocess.CalledProcessError as e:
            logging.error(f"Error downloading dataset {dataset_id}: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Unexpected error downloading dataset {dataset_id}: {str(e)}")
            return False
    
    logging.info("Completed all Copernicus data downloads")
    return True

def run():
    """
    Pipeline module for downloading Copernicus Marine data for August to October of specified years.
    
    Args:
        params (dict): Module parameters
        pipeline_state (dict): Current pipeline state
        global_params (dict): Global pipeline parameters
        
    Returns:
        dict: Updated pipeline state
    """

    boundary_box = {
        'lat_min': 24.0,
        'lat_max': 31.0,
        'lon_min': -92.0,
        'lon_max': -86.0
    }

    years = [2005,2006,2012,2013,2015,2021]


    logging.info("Starting Copernicus data download module")
    
    for year in years:
        success = download_copernicus(boundary_box, year)
        if not success:
            raise Exception(f"Failed to download Copernicus data for {year}")
    
    logging.info("Completed Copernicus data download module")

if __name__ == "__main__":
    run()
