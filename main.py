# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 21:02:55 2023

@author: solis
"""

try:
    from datetime import date
    from time import time
    import traceback
    
    from aemet_open_data import AemetOpenData
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(0)

    
if __name__ == "__main__":

    startTime = time()

    try:
         
        var_type = 'day'
        d1 = date(2000, 1, 1)
        d2 = date(2023, 1, 1)
        estaciones = ('7178I', '7002Y')
        dir_output = './download'
        metadata = False
        verbose = True
        use_files = True
        
        files2exclude = ['7002Y_20091231T000000UTC_20141230T235959UTC.csv',
                         '7002Y_20141231T000000UTC_20191230T235959UTC.csv']
        
        aod = AemetOpenData()
        
        print('Aemet OpenData')
        
        # ========= climatological stations =================================
        overw = input('Want to download the characteristic of the'
                      ' meteorological stations? (y/n): ')
        if overw.lower() in ('y', 'yes', 'si', 'sí', '1'):
            aod.estaciones_climatologicas(dir_output, metadata = metadata)

        # ========= daily or monthly data depending on var_type =============
        overw = input('Want to download the meteorological data? (y/n): ')
        if overw.lower() in ('y', 'yes', 'si', 'sí', '1'):        
            file_names =\
                aod.variables_clima_estacion(var_type, d1, d2, 
                                          estaciones, dir_output,
                                          metadata = metadata, 
                                          verbose = verbose,
                                          use_files = use_files)
            print('Downloaded files', len(file_names))

        # ========= concatenate previously downloaded files =================
        overw = input('Want to concatenate downloaded csv files? (y/n): ')
        if overw.lower() in ('y', 'yes', 'si', 'sí', '1'):
            unique_file = f'meteo_data_{var_type}.csv'
            cfiles = aod.concatenate_files\
                (var_type, dir_output, unique_file)            
        print('Concatenated files', len(cfiles))
        for f1 in cfiles: 
            print(cfiles)

    except ValueError:
        msg = traceback.format_exc()
        logging.append(msg)
    except Exception:
        msg = traceback.format_exc()
        logging.append(msg)
    finally:
        logging.dump()
        xtime = time() - startTime
        print(f'El script tardó {xtime:0.1f} s')

