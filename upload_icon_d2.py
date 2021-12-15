# -*- coding: utf-8 -*-
"""
Created on Tue Mar  3 21:15:47 2020

Script to find the latest ICON-EU climatic data, and upload these to Lizard.
Steps include finding the latest update hour, downloading and decompressing
the files, converting the files to GeoTiffs and uploading these to Lizard.

@author: Martijn.Krol


 This script is modified from :https://github.com/nens/incubed-wam/blob/master/scripts/uploadICON.py for 
 ICON EU data which made by Martijn.Krol, and Kuan-wei.Chen later on modified it for ICON D2 data.

This script only focus on tot_prec for icon d2 and the following changes

The filename is different from ICON EU, so it parse diffrently the filename to get hour and date
aggregation is not included since we will use hourly data for precipitation
The previous script will create 4 bands raster, even though it can still be uploaded to Lizard, but the file size is bigger. 
use new_meta.update({'nodata': -9999, 'driver': driver,'count':1}) to change it to 1 band in the processed step.
Check data every 3 hours instead of 6 hours
ICON D2 tot_prec has "regulalr-lat-lon" and "icosaheral" opetions. The script cannot process unstructured grid, 
only regular-lat-lon data is used


"""

## Import required libraries --------------------------------------------------

# Libraries required for the script to get ICON data from the ftp and upload
# these to Lizard are mainly used to webscraping, zipping, downloading, posting
# and some other basic (spatial) functionality. Localsecret.py is required for
# the credentials to Lizard. This is a Nelen & Schuurmans module.

from bs4 import BeautifulSoup
import bz2
from datetime import datetime, timedelta
import localsecret
import glob
import numpy as np
import os
import rasterio as rio
import gdal
import requests
import re
import logging
import urllib.request

## Login function Lizard ------------------------------------------------------

# The login credentials are required to post data to the Nelen & Schuurmans
# Lizard account. Downloading ICON data does not require any credentials.

LOGIN = {"username": localsecret.user_ns, "password": localsecret.pass_ns}

## Url for ICON-EU data (Grib format) -----------------------------------------

# ICON weather is available at https://opendata.dwd.de/weather/. We generally
# use the numeric weather prediction (NWP) made by the ICON model for Europe.
# This is available as GRIB files, giving the extension /nwp/icon-eu/grib.
# Other options are other models (e.g. COSMO-D2), or data at global extent.
# Data at global extent should be available at /nwp/icon/grib. Data is made
# available for all modelled parameters at 00, 06, 12 and 18hours. Output at
# other moments does not include all parameters. The full ICON documentation is
# available: https://www.dwd.de/DWD/forschung/nwv/fepub/icon_database_main.pdf

# DWD opendata URL from where we download our data
# url = "https://opendata.dwd.de/weather/nwp/icon-eu/grib/"
url = "https://opendata.dwd.de/weather/nwp/icon-d2/grib/"

# Matching units to the UUIDs that are created on Lizard. These should have
# a temporal resolution of 1 day. ICON forecasts have a temporal resolution of 1 hour
# so we aggregate them into daily timesteps. 

uuidmatcher = {
    "precip": "8f0339bc-1378-4df1-b536-ed43ec4d8f87", # Processes into hourly values from cumsum
}
# Base URL of the Lizard v4 API rasterstore
base = "https://demo.lizard.net/api/v4/rasters/"

## Logging procedure ----------------------------------------------------------

# As this script runs operational, a logfile is created to check on performance
# This file will keep track of which units and timesteps have succesfully been
# downloaded, extracted, aggregated  and uploaded to Lizard. 

# Loglevel info
loglevel = logging.INFO

# Generate logging
def configure_logger(loglevel):
    logger = logging.getLogger("ICONlogger")
    logger.setLevel(loglevel)
    ch = logging.FileHandler("./logging/ICON.log", mode="w+")
    # ch = pi.DiagHandler('../logs/diags.txt')
    ch.setLevel(loglevel)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    

## Find latest available data every 6 hours -----------------------------------
    
# This function will check that the latest timestep is at which the data has
# been updated on the DWD site. The concept is to check the different hours to
# see which one has files with the latest date. So if the ICON data at 12hours
# has a newer date than the data at 18hours, this will be taken as latest data.
    
def latestICON(url, unit):

    # Start date to ensure data is always uploaded
    datp = datetime.strptime("1970-01-01", "%Y-%m-%d").date()
    
    # getLogger to start logging
    logger = logging.getLogger("ICONlogger")
    
    # Loop through the hours that have full datasets
#     for h in [18, 12, 6, 0]:
    for h in [21, 18, 15, 12, 9, 6, 3, 0]:  
        logger.debug(h)
        # format hour to format used in URL
        hour = f"{h:02d}"
        # create ICON url for given hour and unit
        hurl = url + hour + "/" + unit + "/"
        # use urllib to open the URL
        data = urllib.request.urlopen(hurl)
        # create empty list to add data to
        hdat = []

        # Get date associated with each file
        for line in data:
            hstr = str(line)
            # only include lines over 180 characters to exclude other data
            if len(hstr) > 180:
                # find 8 digit string, which is the format used for the time
                datm = re.search(r"\d{8}", hstr).group()
                
                # convert timstep to date
                date = datetime.strptime(datm, "%Y%m%d").date()
                # append date to hdat list
                hdat.append(date)
                
        # take first line as latest date (not really efficient...)
        datn = hdat[0]
        logger.debug(datn)

        # Overwrite hour if data is from more recent date. If the date found 
        # for the hour (from latest to first) is newer than for the previous 
        # hour, this hour contains newer data. So this hour will be used.
        if datn > datp:
            datp = datn
            newh = hour

    # Return latest hour that has been updated
    return datp, newh


## Get (download) latest available data ---------------------------------------
    
# This function will download the latest ICON files (based on the previous 
# function), and will convert these to geoTIFF in the WGS84 projection. These
# files will be saved with the unit and timestamp (ISO format) in the name. 
# The next step is to upload these hourly geoTIFFs to Lizard by posting these
# to the correct UUID and providing the ISO-formatted timestamp.
    
def downloadICON(url, unit):

    # Start logger
    logger = logging.getLogger("ICONlogger")
    logger.info("Start downloading {} data".format(unit))
    # Get latest data
    latestdate, latest = latestICON(url=url, unit=unit)
    logger.info("Last ICON {} dataset is of {} {}:00".format(unit, latestdate, latest))
    
    # create ICON request URL for latest hour for given unit
    
    #print(latestdate, latest) >>2021-11-29 03

    req_url = url + latest + "/" + unit + "/"   ##
#     print(req_url)   https://opendata.dwd.de/weather/nwp/icon-d2/grib/03/rain_con/

    # GET data from the created URL
    page = requests.get(req_url)
    
    # Convert data to text
    data = page.text
    
    # convert data to soup with beautiful soup
    soup = BeautifulSoup(data, features="lxml")

    # Find all links on website with beautiful soup
    for link in soup.find_all("a"):
        
        # Get link
        fnam = link.get("href")

        # Only use relevant links (links with few characters can be basic
        # links used to return to previous page or open some document)  
#         if len(fnam) > 70:
        if len(fnam) > 70 and 'regular' in fnam:   #only chooose regular lat-lon
            

            # Get base-hour of datasets. Each link includes the t0 for the 
            # prediction, after which a number is given for the hours after
            # this t0.  
            #  file name example: icon-d2_germany_regular-lat-lon_single-level_2021120100_001_2d_tot_prec.grib2
            hours = re.search(r"\d{10}", fnam).group()   #find 2021120100

            # Get hours after base-hour (t0)
#             delta = int(re.findall(r"\d+", fnam)[1])
            delta = int(re.findall(r"\d{3}", fnam)[-1])   # find 001, 002 etc

            # Create a new timestep for the modelled hour (h t0 + delta h)
            times = datetime.strptime(hours, "%Y%m%d%H") + timedelta(hours=delta)
            # Convert to ISO hour (see https://en.wikipedia.org/wiki/ISO_8601)
            stamp = times.strftime("%Y%m%dT%H%M%SZ")
            logger.debug("Retrieving {} data for {}".format(unit, stamp))

            # Create download link
            file = req_url + fnam
            
            # Retrieve data from the download location
            urllib.request.urlretrieve(file, filename="./data/ICON/" + fnam)
            
            # Open the zipfile with bz2
            zipfile = bz2.BZ2File("./data/ICON/" + fnam)  
            
            # Read to get decompressed data
            data = zipfile.read()  
            
            # Create new filepath to write data to (assume ending on .bz2)
            newfilepath = "./data/ICON/" + fnam[:-4]  
            with open(newfilepath, "wb") as f:
                f.write(data)  # write unzipped file
            
            logger.debug("Writing {} data at {} to tif".format(unit, stamp))
            # Convert GRIB format to GeoTIFF
            dst_ds = "./data/ICON/" + unit + "-" + stamp + ".tif"
            src_ds = gdal.Open(newfilepath)
            bounds = [-3.94, 58.08, 20.34, 43.18]  #use new bounds
            ds = gdal.Translate(
                dst_ds,
                src_ds,
                outputSRS="EPSG:4326",
                outputBounds=bounds,
                noData=-9999,
                format="GTiff",
            )
            
            src_ds = None  # Close files
            ds = None
            data = None
            zipfile = None
            logger.debug("Retrieval of {} data at {} complete".format(unit, stamp))
    
    logger.debug("Deleting processed files")
    files = sorted(glob.glob("data/ICON/*.bz2") + glob.glob("data/ICON/*.grib2"))
    for f in files:
#         pass
        os.remove(f)
        
    logger.info("Download of {} data succesful".format(unit))
                
## Processing of downloaded files to relevant products ------------------------
                
# For precipitation the cumsum is changed to hourly values.
                
def processICON(prod):   
    logger = logging.getLogger("ICONlogger")
    logger.info("Start processing {} data".format(prod))
    # Hourly precipitation ----------------------------------------------------
    if prod == 'precip':
        unit = 'tot_prec'    
        filelist = sorted(glob.glob(os.path.join("data","ICON",unit + "*")))
        for rfile in filelist:       
            tstmp =  rfile[-20:-4]  
            dst_nw = "./data/ICON/{}-{}.tif".format(prod,tstmp)

            with rio.open(rfile) as nrc:    
                nrc_ds = nrc.read(1, masked=True) 
            if rfile == filelist[0]:
                old_ds = nrc_ds
                continue #Dont upload first timestamp as it contains only zeros
            else:
                new_ds = nrc_ds - old_ds
                old_ds = nrc_ds         
            # Create dictionary copy
            new_meta = nrc.meta.copy()
            driver = 'Gtiff'
               # Update the nodata value to be an easier to use number
                
                
        ###change band to 1 (count =1)
            new_meta.update({'nodata': -9999, 'driver': driver,'count':1})
            with rio.open(dst_nw, 'w', **new_meta) as outf:
                outf.write(new_ds, 1)
        #Delete old files
        for x in filelist:
#             pass
            os.remove(x)  
        
        logger.info("Precipitation processing complete")

        
    else:
        logger.warning("Product {} is not recognized".format(prod))


# Upload to lizard ------------------------------------------------------------

# The created tiff will be uploaded to the Lizard rasterstore for the relevant
# product. This is based on the rasterstore base-url and the UUID of the data.
                
def uploadICON(prod, base, uuid, LOGIN):   
    logger = logging.getLogger("ICONlogger")
    logger.info("Start uploading {} data".format(prod))       
    # Paste url and UUID to get upload location for rasters
    purl = base + uuid + "/data/"
            
    # Get files
    rlist = sorted(glob.glob("data/ICON/" + prod + "*.tif"))  #use ICON instead of ICON_daily

    for file in rlist:       
        # Add timestamp to data sent to url
        tstmp = file[-20:-4]
        data = {"timestamp": tstmp}
        logger.debug("Uploading {} data at {}".format(prod, tstmp))   
         
        # Open the created geoTIFF (WGS84 format)
        file = {"file": open(file, "rb")}
                
        # Post the data and include the timestamp and credentials
        r = requests.post(url=purl, data=data, files=file, headers=LOGIN)
        r.raise_for_status()
        logger.debug("Upload of {} at {} complete".format(prod, tstmp))
        
    logger.info("Uploading of {} data complete".format(prod))
    
 
 ## Run function ---------------------------------------------------------------
            
# The below section is used to run the script in an operational manner. This
# includes the logging of the steps, and cleaning of data after uploading. This
# part of the script is created by Martijn Krol.
            
configure_logger(loglevel)
logger = logging.getLogger("ICONlogger")
logger.info("Starting script")
os.makedirs(os.path.join("data","ICON"),exist_ok=True)


### Precipitation test 
 
try:
    downloadICON(url, "tot_prec")
    processICON("precip") #Process to hourly values
    uuid = uuidmatcher["precip"]
    uploadICON("precip", base, uuid, LOGIN)
except Exception as e:
    print(e)
    logger.error("Precipitation processing failed", exc_info=True)
     
     
 #Remove excess files after upload ---------------------------------------------------
logger.info("Removing uploaded files")
files = sorted(glob.glob("data/ICON/*"))
for f in files:
    os.remove(f)
#     pass

logger.info("Script complete")

## End ------------------------------------------------------------------------


