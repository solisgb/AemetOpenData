# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 09:38:41 2023

@author: solis
"""
from datetime import date

# Here you set the values of the parameters of the methods used.
# For more information see the docstring of the methods in
# aemet_open_data.py

         
# To download characteristics of meteorological stations in Aemet OpenData
#  we use the method meteo_stations. It has the following parameters:
    
dir_path = '.\download\data_all_stations'  # files will be saved here
data_request = 'both'  # 'data', 'metadata' or 'both'
use_files = True  # prevents downloading files that have already been downloaded   
verbose = True  # messages in screen


# Common parameters of all methods that download meteorological data
d1 = date(1985, 1, 1)  # start date
d2 = date(1985, 12, 31)  # end date


# To download daily data from all meteorological stations we use the method
#  meteo_data_all_stations
# There are no new parameters with respect to those defined above. 


# To download data from some meteorological stations we use the method
#  data_meteo_stations. 
# Specific parameters:

var_type = 'day'  # 'day' or 'month'
estaciones = ('7178I', '7002Y')  # list with station identifiers
metadata = False  # if True data will be downloaded, otherwise metadata


# To concatenate the CSV files with the meteorolofical data downloaded using
#  meteo_data_all_stations or data_meteo_stations, we use the method
#  concatenate_files. Specific parameters:
 
files2concat = []  # if you want to concatenate some files only
files2exclude = []  # if yoy want to exclude some files
ask_overwrite = True  # if the final file already exists ask before overwriting 
        

        