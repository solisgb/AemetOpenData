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
    
dir_path = './download/data_selected_stations'  # files will be saved here
fetch = 'both'  # 'data', 'metadata' or 'both'
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

time_step = 'day'  # 'day' or 'month'
stations = ['7178I', '7031', '7031X']  # list with station identifiers


# To save csv files to Sqlite db and optionally to a single csv we use the
#  method AOP_2db.to_db
# Specific parameters:

# ftype of request to Aemet: 
#    'stations_day'. Daily meteorological data from all stations
#    'station1_day'. Daily meteorological data from selected stations
#    'station1_month'. Monthly meteorological data from selected stations

ftype = 'stations_day'
point_dec_sep = True  # decimal separator for float data: point or comma



        