# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 21:02:55 2023

@author: solis
"""

try:
    from time import time
    import traceback
    
    from aemet_open_data import AemetOpenData
    import aemet_open_data_parameters as par 
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(1)

    
if __name__ == "__main__":

    startTime = time()
    
    YES_ANSWERS = ('y', 'yes', 'si', 'sí', '1')

    try:
         
        global_vars = {name: value for name, value in par.__dict__.items() \
                       if not name.startswith("__")}

        print('Aemet OpenData')
        print('Parameters are assigned in aemet_open_data_parameters.py')
        
        # Print the variables and their values
        nv = [(name, value) for name, value in global_vars.items() \
              if name != 'date']
        print('Parameters')
        for nv1 in nv:    
            print(f"'{nv1[0]}': {nv1[1]}")
        ans = input('\nContinue with these values? (y/n): ')
        if ans.lower() not in YES_ANSWERS:
            raise SystemExit(0)
            
        aod = AemetOpenData()
        
        # ========= meteorological stations =================================
        ans = input('Want to download the characteristic of the'
                    ' meteorological stations? (y/n): ')
        if ans.lower() in YES_ANSWERS:
            aod.meteo_stations(par.dir_output, metadata = par.metadata)

        # ========= daily or monthly data depending on var_type =============
        ans = input('Want to download the meteorological data? (y/n): ')
        if ans.lower() in YES_ANSWERS:        
            file_names =\
                aod.data_meteo_stations(par.var_type, par.d1, par.d2, 
                                        par.estaciones, par.dir_output,
                                        metadata = par.metadata, 
                                        verbose = par.verbose,
                                        use_files = par.use_files)
            print('Downloaded files', len(file_names))

        # ========= concatenate previously downloaded files =================
        overw = input('Want to concatenate downloaded csv files? (y/n): ')
        if overw.lower() in YES_ANSWERS:
            cfiles = aod.concatenate_files(par.var_type, par.dir_output,
                                           par.files2concat,
                                           par.files2exclude, 
                                           par.ask_overwrite)            
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

