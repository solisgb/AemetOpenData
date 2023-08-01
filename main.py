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
         
        var_type = 'month'
        d1 = date(2000, 1, 1)
        d2 = date(2023, 1, 1)
        estaciones = ('7178I', '7002Y')
        dir_output = './download'
        metadata = False
        verbose = True
        use_files = True
        
        aod = AemetOpenData()
        

        # ========= climatological stations =================================
        # aod.estaciones_climatologicas(dir_output, metadata = metadata)


        # ========= daily or monthly data depending on var_type ============= 
        file_names =\
            aod.variables_clima_estacion(var_type, d1, d2, 
                                      estaciones, dir_output,
                                      metadata = metadata, verbose = verbose,
                                      use_files = use_files)
        print('Ficheros descargados', len(file_names))

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

