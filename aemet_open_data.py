# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 10:15:17 2019

@author: solis
"""
try:
    from calendar import isleap
    from datetime import date, datetime, timedelta
    import inspect
    import pandas as pd
    import pathlib
    import re
    import requests
    from requests.adapters import HTTPAdapter, Retry
    from typing import Union
    
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(0)


class AemetOpenData():
    """
    Descarga datos climatológicos de Aemet OpenData
    Interfaz pública:
        daily_ranges_get
        years_ranges_get
        climatologias_estaciones
        climatologias_diarias
        climatologias_mensuales
    """

    # type aliases to use in typing.Union
    __TupStr = tuple[str]
    __LisStr = list[str]

   # code_value of Aemet server 
    _RESPONSEOK = 200
    _UNAUTHORIZED = 401
    _NOTFOUND = 404
    _TOOMANYREQUESTS = 429
    
    # request constants
    _MAXREQUEST = 3
    _BACKOFF_FACTOR = 3
    _TIMEOUT = (2, 5)
    
    # limits in Aemet server (time series request)
    _MAX_NDAYS_ESTACION = 365*5
    _MAX_NYEARS_ESTACION = 3
    
    
    def __init__(self, file_name: str='apikey.txt'):
        # Reads a valid apikey from file
        with open(file_name) as f:
            self.__myapikey = f.readline()
        self.__querystring = {"api_key": self.__myapikey}
        self.__headers = {'cache-control': "no-cache"}


    def get_public_methods(self):
        return [name for name, _ in inspect.getmembers(self) \
                if not name.startswith('_')]


    @staticmethod
    def directory_exists(d_name: str) -> bool:
        """
        Asks the question if the directory path d_name exists or not

        Parameters
        ----------
        d_name : directory path as str to check

        Returns
        -------
        bool
        """
        directory_path = pathlib.Path(d_name)
        if directory_path.exists() and directory_path.is_dir():
            return True
        else:
            return False


    @staticmethod
    def file_exists(d_name: str, f_name: str) -> bool:
        """
        Asks the question if the file fname in the directory path d_name
            exists or not

        Parameters
        ----------
        d_name : directory path as str
        f_name : file name as str to check
        Returns
        -------
        bool
        """        
        d_path = pathlib.Path(d_name)
        file = d_path.joinpath(f_name) 
        if file.exists() and file.is_file():
            return True
        else:
            return False


    @staticmethod 
    def file_names_in_dir(d_name: str, pattern: str) -> [str]:
        if not AemetOpenData.directory_exists(d_name):
            msg = '{d_name} is not a directory'
            logging.append(msg)
            raise ValueError(msg)
        d_path = pathlib.Path(d_name)
        file_paths = d_path.glob(pattern)
        file_names = [f1.name for f1 in file_paths]
        return file_names


    @staticmethod
    def __file_name_clean(fname: str):
        fname = fname.replace('-', '')
        fname = fname.replace(':', '')
        return fname


    @staticmethod
    def __swap_ts_limits(d1, d2):
        if d1 > d2:
            tmp = d1
            d1 = d2
            d2 = tmp
            logging.append(f'{d1} - {d2} swapped')
        return (d1, d2)


    @staticmethod
    def __shrink_ts_limits(d1: Union[int, date, datetime], 
                           d2: Union[int, date, datetime]):
        LOWEST_YEAR = 1900
        now = date.today()
        
        d1, d2 = AemetOpenData.__swap_ts_limits(d1, d2)
        
        if isinstance(d1, int):
            if d1 < LOWEST_YEAR:
                d1 = LOWEST_YEAR
            if d2 > now.year:
                d2 = now.year
        else:
            if d1.year < LOWEST_YEAR:
                if isleap(d1.year) and d1.month == 2 and d1.day == 29:
                    d1 = date(LOWEST_YEAR, 2, 28)
                else:
                    d1 = date(LOWEST_YEAR, d1.month, d1.day)
            if d2.year > now.year:
                if isleap(d1.year) and d1.month == 2 and d1.day == 29:
                    if isleap(now.year):
                        d2 = date(now.year, 2, 29)
                    else:
                        d2 = date(now.year, 2, 28)
                else:
                    d2 = date(now.year, now.month, now.day)
        return (d1, d2)


    @staticmethod
    def daily_ranges_get(d1: Union[date, datetime], 
                         d2: Union[date, datetime]) -> ((str, str)):
        """
        Generates a list of date ranges as days between d1 and d2 with a
            maximum duration max_ndays each; the dates in days_range are str
            in the format of the Aemet OpenData API.
        """
        if type(d1) != type(d2):
            raise TypeError('d1 and d2 must have the same type')

        if not isinstance(d1, (date, datetime)):
            raise TypeError('d1 and d2 must be of type date or datetime')
        
        max_ndays = timedelta(AemetOpenData._MAX_NDAYS_ESTACION)
        one_day = timedelta(1) 
        cd = d1
        today = date.today()
        days_range = []
        for i in range(5000):
            diff = d2 - cd
            scd = cd.strftime('%Y-%m-%dT00:00:00UTC')
            if diff > max_ndays:
                du = cd + max_ndays
                sdu = du.strftime('%Y-%m-%dT23:59:59UTC')
                days_range.append((scd, sdu))
            else:
                du = cd + diff
                sdu = du.strftime('%Y-%m-%dT23:59:59UTC')
                days_range.append((scd, sdu))
                break
            cd = du + one_day
            if cd > today:
                break
        return days_range


    @staticmethod
    def years_ranges_get(y1: Union[int, date, datetime], 
                         y2: Union[int, date, datetime]) -> ((str, str)):
        """
        Generates a series of year ranges years_range between y1 and y2 with a
            maximum duration max_nyears each; the years in years_range are
            4-digit strings required by the Aemet OpenData API.
        """
        if type(y1) != type(y2):
            raise TypeError('y1 and y2 must have the same type')
        
        if not isinstance(y1, (int, date, datetime)):
            raise TypeError('y1 and y2 must be of type int or date or datetime')
        
        if isinstance(y1, (date, datetime)):
            y1 = y1.year
            y2 = y2.year
        
        y1, y2 = AemetOpenData.__swap_ts_limits(y1, y2)
        
        cy = y1
        AemetOpenData._MAX_NYEARS_ESTACION
        years_range = []
        for i in range(5000):
            diff = y2 - cy
            scy = str(cy)
            if diff > AemetOpenData._MAX_NYEARS_ESTACION:
                yu = cy + AemetOpenData._MAX_NYEARS_ESTACION
                syu = str(yu)
                years_range.append((scy, syu))
            else:
                yu = cy + diff
                syu = str(yu)
                years_range.append((scy, syu))
                break
            cy = yu + 1
        return years_range


    def __request_get(self, url: str, s: requests.Session)\
        -> requests.models.Response:
        """
        It makes a get request
        Parameters
        ----------
        url : url.
        s : requests.Session

        Raises
        ------
        SystemExit
        Returns
        -------
        r : requests.models.Response
        """

        try:
            r = s.get(url, headers=self.__headers, params=self.__querystring,
                      timeout=AemetOpenData._TIMEOUT)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as err:
            msg = f'HTTPError, code{r.status.code}: {r.reason}'
            logging.append(msg)
            raise SystemExit(err)
        except Exception as err:
            msg = f'Request error {err}'
            logging.append(msg)
            raise SystemExit(err)        

    
    def __data_get(self, r: requests.models.Response, metadata: bool=False) \
        -> (pd.DataFrame, str):
        """
        It downloads data from the urls in r['datos'] or r['metadatos'] 
        depending on metadata parameter value; Key values 'datos' and 
        'metadatos' are specific of Aemet OpenData

        Parameters
        ----------
        r : requests.models.Response
        metadata : if True downloas metadata, otherwise data
        Raises
        ------
        SystemExit
        Returns
        -------
        (df with data or metadata or len 0 dataframe, a description provided by
         Aemet)
        """
        VOID_DATAFRAME = pd.DataFrame({'c':[]})
        
        # defined by Aemet
        DATA_KEY = 'datos'
        METADATA_KEY = 'metadatos'
        DESCRIPTION_KEY = 'descripcion'
        
        try:
            r_dic = r.json()
        except requests.exceptions.JSONDecodeError as err:
            msg=f'JSON decode error {err}'  
            logging.append(msg)
            raise SystemExit(err) 
        except Exception as err:
            msg=f'Error {err}'  
            logging.append(msg)
            raise SystemExit(err) 
        
        if r.status_code == 200:
            if metadata:
                field = METADATA_KEY
            else:
                field = DATA_KEY
            if field in r_dic:
                try:
                    df = pd.read_json(r_dic[field], encoding='ISO-8859-15')
                except Exception as err:
                    msg=f'Error reading json from url: {err}'  
                    logging.append(msg)
                    raise SystemExit(err)                         
            else:
                df = VOID_DATAFRAME
            if DESCRIPTION_KEY in r_dic:
                description = r_dic[DESCRIPTION_KEY]
            else:
                description = ''
        else:
            df = VOID_DATAFRAME
            description = ''
        return (df, description)


    def __request_meteo_data\
        (self, dr: [(str, str)], 
         estaciones: Union[__TupStr, __LisStr, str], mtype: str, 
         dir_name: str=None, metadata: bool=False, verbose: bool=True) \
            -> (pd.DataFrame, str): 
        """
        Makes one or many request to the server and downloads the data 
            in one or many files depending on the range of time series 
            requested, in relation to the limits defined by Aemet for each
            request.
        It returns the data as an iterator using yield
        Parameters
        ----------
        dr : date or year ranges to do valid requests
        stations: tuple of station codes or code of a station
        mtype : according to its content, indicates whether the climatological
            variables are requested with daily or monthly discretization
            (valid_mtypes)
        metadata: If True only metadata are downloaded, otherwse only data
            are downloaded
        dir_name. If it is none, it checks in the directory if the file 
            corresponding to the request to be made already exists and if it
            does, it will not make the data request to the server; if it is
            none, this check is not made and the request to the server is
            always made, the data is downloaded and the existing file is
            overwritten.
        verbose: If True shows information of results request in the screen
        Raises
        ------
        ValueError
        Returns
        -------
        Iterator: each iteration returns 
        (df with data or metadata or len 0 dataframe, a server description 
             of the response provided by the server)
        """
        
        """
        If you modify the pattern of fnames, you must review the regex
            patterns in concatenate_files method
        """
        
        valid_mtypes = ('day', 'month')
        if mtype not in valid_mtypes:
            msg = ', '.join(valid_mtypes)
            raise ValueError(f'mtype not in {msg}')

        if dir_name is None:
            file_names = []
        else:
            file_names = AemetOpenData.file_names_in_dir(dir_name, '*.csv')

        s = requests.Session()
        retries = Retry(total=AemetOpenData._MAXREQUEST, 
                        backoff_factor=AemetOpenData._BACKOFF_FACTOR, 
                        status_forcelist=[ AemetOpenData._TOOMANYREQUESTS ])
        s.mount('http://', HTTPAdapter(max_retries=retries))

        for e1 in estaciones:
            if not verbose:
                logging.append(f'Station {e1}')
            for dr1 in dr:
                
                if mtype == 'day':
                    url = 'https://opendata.aemet.es/opendata/api/valores/'+\
                        'climatologicos/diarios/datos/fechaini/'+\
                            f'{dr1[0]}/fechafin/{dr1[1]}/estacion/{e1}'
                else:
                    url = 'https://opendata.aemet.es/opendata/api/valores/'+\
                        'climatologicos/mensualesanuales/datos/anioini/'+\
                            f'{dr1[0]}/aniofin/{dr1[1]}/estacion/{e1}'

                if metadata:
                    f_name = 'climatologias_diarias_metadata.csv'
                else:
                    f_name = f'{e1}_{dr1[0]}_{dr1[1]}.csv'
                    f_name = AemetOpenData.__file_name_clean(f_name)

                if dir_name is not None:
                    if f_name in file_names:
                        msg = f'{f_name} has been previously downloaded'
                        logging.append(msg)
                        if metadata:
                            break
                        else:
                            continue

                r = self.__request_get(url, s)
                df, description = self.__data_get(r, metadata)

                if verbose:
                    msg = f'{e1}, {dr1[0]}, {dr1[1]}: '+\
                    f'{r.status_code}, {r.reason} {description}'
                    logging.append(msg)                                                 
                
                yield (df, f_name)
                    
                if metadata:
                    break
                
            if metadata:
                break


    def meteo_stations(self, dir_out: str, metadata: bool=False) \
        -> str:
        """
        Retrieves characteristics of meteorological stations in Aemet
            OpenData and saves them to a file

        Parameters
        ----------
        dir_out: The name of the directory where the data file will be saved
        metadata: If True only metadata will be saved, otherwise only data
        will be saved

        Returns
        -------
        file name with data or metadata
        """
        FNAME = 'estaciones_open_data'
        URL = 'https://opendata.aemet.es/opendata/api/valores/'+\
            'climatologicos/inventarioestaciones/todasestaciones'

        if not AemetOpenData.directory_exists(dir_out):
            logging.append(f'Directory {dir_out} not exists')
            return
        dir_path = pathlib.Path(dir_out)
        
        s = requests.Session()
        retries = Retry(total=AemetOpenData._MAXREQUEST, 
                        backoff_factor=AemetOpenData._BACKOFF_FACTOR, 
                        status_forcelist=[ AemetOpenData._TOOMANYREQUESTS ])
        s.mount('http://', HTTPAdapter(max_retries=retries))

        r = self.__request_get(URL, s)
        df, description = self.__data_get(r, metadata)        
        if len(df) > 0:
            if metadata:
                file_name = FNAME + '_metadata.csv'
            else:
                file_name = FNAME + '.csv'
            file_path = dir_path.joinpath(file_name)
            df.to_csv(file_path, index=False)
            logging.append(f'Downloaded data in {file_name}')
       
        return file_name


    @staticmethod
    def __validate_var_type_param(var_type: str):

        VALID_VAR_TYPES = ('day', 'month')
        if var_type not in VALID_VAR_TYPES:
            msg = ', '.join(VALID_VAR_TYPES)
            raise ValueError(f'var_type not in {msg}')                    
        

    @staticmethod 
    def __data_meteo_stations_check_type_parameters\
        (var_type, d1, d2, estaciones, dir_out, metadata, verbose, use_files):
        """
        Check the parameter types of method variables_clima_estacion
    
        Raises
        ------
        TypeError
        
        Returns
        -------
        bool
        """
            
        if not isinstance(var_type, str):
            logging.append("Parameter 'var_type' must be str")
            return False
 
        if type(d1) != type(d2):
            logging.append('types for d1 and d2 must be equal')
            return False
        if not isinstance(d1, (int, date, datetime)):
            logging.append('Incorrect type for d1 and d2')
            return False
        
        if not isinstance(estaciones, (tuple, list, str)):
            logging.append("Parameter 'estaciones' must be () or [] or str")
            return False
        
        if isinstance(estaciones, (tuple, list)):
            not_str = []
            for i, e1 in enumerate(estaciones):
                if not isinstance(e1, str):
                    not_str.append(str(i))
            if not_str:
                msg = ', '.join(not_str)
                logging.append("Parameter 'estaciones' has not str"+\
                                f" members at positions {msg}")
                return False
        
        variables = {'metadata': metadata, 'verbose': verbose,
                     'use_files': use_files}
        for var_name, var_value in variables.items():
            if not isinstance(var_value, bool):
                logging.append("Parameter {var_name}' must be bool")
                return False

        return True


    def data_meteo_stations(self, var_type: str, d1: date, d2: date, 
                            estaciones: Union[__TupStr, __LisStr, str],
                            dir_out: str,  metadata: bool=False,
                            verbose: bool=True, use_files: bool=True) -> [str]:
        """
        Retrieves daily or monthly climatological data from Aemet OpenData
            server.

        Parameters
        ----------
        var_type : day for daily variables, month for monthly ones
        d1 : Initial date (daily data) or year (monthly data)
        d2 : Final date or year. Equal d1
        estaciones : List of stations or only one station as str
        dir_out : Each request to Aemet server is saved as a csv file in the
            directory dir_out        
        metadata: If True only metadata are downloaded, otherwise only data
            are downloaded
        verbose: If True shows detailed information of results request in the
            screen
        use_files : If True checks that the file name already exists in dir_out
            and does not make the request to the server; otherwise the request
            is made and the pre-existing file is overwritten.
        Returns
        -------
        [] with the the names of saved csv files. Each file has a subset of
            all data downloaded
        """

        if not AemetOpenData.__data_meteo_stations_check_type_parameters\
            (var_type, d1, d2, estaciones, dir_out, metadata, verbose,
             use_files):
            return []

        AemetOpenData.__validate_var_type_param(var_type)

        d1, d2 = AemetOpenData.__shrink_ts_limits(d1, d2)
        
        if isinstance(estaciones, str):
            estaciones = (estaciones,)
            
        file_names = []
        if not AemetOpenData.directory_exists(dir_out):
            logging.append(f'Directory {dir_out} not exists')
            return file_names
        dir_path = pathlib.Path(dir_out)

        if use_files:
            dir_name = dir_out
        else:
            dir_name = None
        
        if var_type == 'day':
            dr = AemetOpenData.daily_ranges_get(d1, d2)
        else:
            dr = AemetOpenData.years_ranges_get(d1, d2)
        
        for df, fname in self.__request_meteo_data\
            (dr, estaciones, var_type, dir_name, metadata, verbose):
            if len(df) > 0:
                file_path = dir_path.joinpath(fname)
                df.to_csv(file_path, index=False)
                file_names.append(fname)
        return file_names


    @staticmethod 
    def __concatenate_files_check_type_parameters\
        (var_type, dir_name, files2concat, files2exclude, ask_overwrite) \
            -> bool:
        """
        Check the parameter types of method variables_clima_estacion
    
        Raises
        ------
        TypeError
        
        Returns
        -------
        None.
        """
        d = {'var_type': var_type, 'dir_name': dir_name}
        for var, value in d.items():
            if not isinstance(value, str):
                logging.append(f'Parameter {var} must be of type str')
                return False

        if not isinstance(ask_overwrite, bool):
            logging.append('Parameter ask_overwrite must be of type bool')
            return False

        files2 = {'files2concat': files2concat, 
                  'files2exclude': files2exclude}

        for name, value in files2.items():
            if not isinstance(value, (list, tuple)):
                logging.append(f'Parameter {name} must be of type [] or ()')
                return False
            not_str = []
            for i, v1 in enumerate(value):
                if not isinstance(v1, str):
                    not_str.append(str(i))
            if not_str:
                not_valid = ', '.join(not_str)
                logging.append(f'Parameter {name} has not str values in'
                               f' positions {not_valid}')
                return False            
        
        return True


    def concatenate_files(self, var_type: str, dir_name: str, 
                          files2concat: [str] = [],
                          files2exclude: [str] = [],
                          ask_overwrite: bool=True) -> [str]:
        """
        Concatenates data csv files in dir_name in a new csv file named
            file_name; data csv files were daily or monthly data files
            saved with the names given in this app; you select the type
            of files to be saved by using var_type

        Parameters
        ----------
        var_type: Day for daily variables, month for monthly ones
        dir_name: Directory path where the files
        files2concat: If len(files2concat) > 0, only concats these files;
            otherwise concats the files that match a regex pattern; this
            pattern differs depending on var_type
        files2exclude: If len(files2concat) == 0 and len(files2exclude) > 0
            the files in files2exclude will not be considered
        ask_overwrite: If file_name exists, ask before overwrite it
        
        Returns
        -------
        List of concatenated csv files
        """
        if not AemetOpenData.__concatenate_files_check_type_parameters\
            (var_type, dir_name, files2concat, files2exclude,
             ask_overwrite):
                return []
        
        AemetOpenData.__validate_var_type_param(var_type)

        if not AemetOpenData.directory_exists(dir_name):
            msg = '{dir_name} is not a directory'
            logging.append(msg)
            return []

        unique_file = f'meteo_data_{var_type}.csv'
        if AemetOpenData.file_exists(dir_name, unique_file):
            if ask_overwrite:
                overw = input(f'File {unique_file} exists, overwrite? (y/n): ')
                if not overw.lower() in ('y', 'yes', 'si', 'sí', '1'):
                    logging.append('{unique_file} has not been overwrited')
                    return []
        
        d_path = pathlib.Path(dir_name)
        concatenated_files = []
        if files2concat:
            for i, f2c1 in enumerate(files2concat):
                f1 = d_path.joinpath(f2c1)
                if f1.exists() and f1.is_file():
                    if i == 0:
                        df = pd.read_csv(f1)
                    else:
                        tmp = pd.read_csv(f1)
                        df = pd.concat([df, tmp], axis=0, ignore_index=True)
                    concatenated_files.append(f1.name)                        
                else:
                    logging.append(f'File {f1} does not exists')
        else:    
            file_paths = d_path.glob('*.csv')
            
            REG_PATTERNS = {'day':  r'(_\d{8}T\d{6}UTC|\.csv)', 
                            'month': r'(_\d{4}_\d{4}|\.csv)'}
    
            first = True
            for f1 in file_paths:
                added = False
                chunks = re.findall(REG_PATTERNS[var_type], f1.name)
                if len(chunks) >= 2:
                    if first:
                        if f1.name not in files2exclude:
                            df = pd.read_csv(f1)
                            added = True
                            first = False
                    else:
                        if f1.name not in files2exclude:
                            tmp = pd.read_csv(f1)
                            df = pd.concat([df, tmp], axis=0,
                                           ignore_index=True)
                            added = True
                    if added:
                        concatenated_files.append(f1.name)                        

        unique_file = f'meteo_data_{var_type}.csv'
        df.to_csv(d_path.joinpath(unique_file), index=False)
        print(f'{unique_file} has been saved')        

        return concatenated_files
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        