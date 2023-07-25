# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 21:02:55 2023

@author: solis
"""

try:
    from datetime import date
    import pandas as pd
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
         
        d1 = date(2000, 1, 1)
        d2 = date(2023, 1, 1)
        estaciones = ('7178I', '7002Y')
        fo = './download/mes.csv'
        
        aod = AemetOpenData()
        
        # fo = './download/estaciones.csv'
        # data = aod.climatologias_estaciones()
        
        # fo = './download/mes.csv'
        # data = aod.climatologias_mensuales(d1, d2, estaciones)
        
        fo = './download/metadata_dia.csv'
        data = aod.climatologias_diarias(d1, d2, estaciones, True)
        
        print('Líneas de datos descargados', len(data))
        if isinstance (data, pd.DataFrame):
            data.to_csv(fo, index=False)

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

