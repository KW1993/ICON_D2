# ICON_D2
Get icon d2 data 

"""
This script is modified from :https://github.com/nens/incubed-wam/blob/master/scripts/uploadICON.py for ICON EU data which made by Martijn.Krol, and Kuan-wei.chen later on modified it for ICON D2 data.

This script only focus on tot_prec for icon d2 and the following changes

1. The filename is different from ICON EU, so it parse diffrently the filename to get hour and date 
2. aggregation is not included since we will use hourly data for precipitation
3. The previous script will create 4 bands raster, even though it can still be uploaded to Lizard, but the file size is bigger.
 use new_meta.update({'nodata': -9999, 'driver': driver,'count':1}) to change it to 1 band in the processed step.
4. Check data every 3 hours instead of 6 hours
5. ICON D2 tot_prec has "regulalr-lat-lon" and "icosaheral" opetions. 
The script cannot process unstructured grid, only regular-lat-lon data is used


"""
