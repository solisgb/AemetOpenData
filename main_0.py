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
            
        adv = 'Daily'
        if par.time_step == 'month':
            adv = 'Monthly'
            
        aod = AemetOpenData()
        
        print('\nOptions')
        print('1. Characteristic of the meteorological stations')
        print('2. Daily data from all meteorological stations')
        print(f'3. {adv} data of selected meteorological stations')
        print('4. Concatenate previously downloaded csv files ')
        ans = input\
            ('\nWrite the number of the option or any other key to quit: ')
        print('')
        if ans == '1': 
            aod.meteo_stations\
                (par.dir_path, par.fetch, par.use_files, par.verbose)
        elif ans == '2':
            file_names =\
                aod.meteo_data_all_stations(par.d1, par.d2, par.dir_path, 
                                            data_request = par.fetch, 
                                            verbose = par.verbose, 
                                            use_files = par.use_files)

            print('Downloaded files', len(file_names))                
        elif ans == '3':
            file_names =\
                aod.meteo_data_by_station(par.time_step, par.d1, par.d2, 
                                          par.estaciones, par.dir_path,
                                          metadata = par.metadata, 
                                          verbose = par.verbose,
                                          use_files = par.use_files)
            print('Downloaded files', len(file_names))
        elif ans == '4':
            cfiles = aod.concatenate_files(par.time_step, par.dir_path,
                                           par.files2concat,
                                           par.files2exclude, 
                                           par.ask_overwrite)            
            print('Concatenated files', len(cfiles))
            for f1 in cfiles: 
                print(cfiles)
        else:
            print('Not an action option selected')

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

