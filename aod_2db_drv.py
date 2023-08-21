# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 11:27:03 2023

@author: solis
"""

try:
    from time import time
    import traceback
    
    from aod_2db import AOD_2db
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(1)

    
if __name__ == "__main__":

    startTime = time()

    try:
        
        dpath = './download'
        ftype = 'stations_day'
        a2db = AOD_2db(dpath, ftype)
        # if not a2db.to_db():
        #     print('Attemp failed')
        a2db.to_csv()
        
        
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
