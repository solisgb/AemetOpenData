# -*- coding: utf-8 -*-
"""
Created on Thu Aug  3 09:38:41 2023

@author: solis
"""
from datetime import date

# Here you set the values of the parameters of the methods used.

         
# To download characteristics of meteorological stations in Aemet OpenData
#  we use the method meteo_stations. It has the following parameters:

var_type = 'day'
metadata = False

# To download data from meteorological stations we use the method
#  data_meteo_stations. Besides the parameters above, this method takes
# the following specific parameters:
 
d1 = date(2000, 1, 1)
d2 = date(2023, 1, 1)
dir_output = './download'
metadata = False
verbose = True
use_files = True

# To concatenate the CSV files with the data downloaded using method 
#  data_meteo_stations, we use the method concatenate_files. In addition to the
#  previously used parameter dir_output, it has the following specific
#  parameters:
 
files2concat = []
files2exclude = []
ask_overwrite = True
        

        