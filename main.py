# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 21:02:55 2023

@author: solis
"""

try:
    from datetime import date
#    import pandas as pd
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
        metadata = True
        use_files = False
        
        aod = AemetOpenData()
        
        """
    def variables_clima_estacion(self, var_type: str, d1: date, d2: date, 
                                 estaciones: Union[(str), [str], str],
                                 dir_out: str,  metadata: bool=False,
                                 verbose: bool=True, use_files: bool=True) \
        -> [str]:
        """
        file_names =\
            aod.variables_clima_estacion(var_type, d1, d2, 
                                     estaciones, dir_output,
                                     metadata)
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

