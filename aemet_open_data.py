# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 10:15:17 2019

@author: solis
"""
try:
    from calendar import isleap
    import csv
    from datetime import date, datetime, timedelta
    import inspect
    import json
    import pathlib
    import requests
    from requests.adapters import HTTPAdapter, Retry
    from time import time
    from typing import Union
    
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(0)
        
        
class ScalarContainer:
    """
    I use this class to pass scalars by reference to a function
    """
    def __init__(self, item):
        self.__x = item


    @property
    def x(self):
        return self.__x


    @x.setter
    def x(self, item):
        self.__x = item


class AemetOpenData():
    """
    Descarga datos meteorológicos del servidor Aemet OpenData
    Download meteorological data from the Aemet OpenData server. 
    Interfaz pública: run get_public_methods()
    """

    # Type aliases to use in typing.Union
    __TupStr = tuple[str]
    __LisStr = list[str]
    __list_dict = list[dict]

   # Response.code_values in requests to Aemet server 
    __RESPONSEOK = 200
    __UNAUTHORIZED = 401
    __NOTFOUND = 404
    __TOOMANYREQUESTS = 429
    
    # Parameters used in Request 
    # After each failed connection attempt, it will sleep for
    #  {backoff_factor} * (2 **(total number of retries - 1)) seconds
    __MAXREQUEST = 5
    __BACKOFF_FACTOR = 5
    # Number of seconds Requests will wait for your client to establish a 
    #  connection to a remote machine call on the socket. It’s a good practice
    #  to set connect timeouts to slightly larger than a multiple of 3, which
    #  is the default TCP packet retransmission window.
    __TIMEOUT = (4, 28)
    
    # Maximum length of time series requests imposed by Aemet 
    __MAX_NDAYS_STATION = 365*5
    __MAX_NDAYS_ALL_STATIONS = 31
    __MAX_NYEARS_STATION = 3
    
    # For methods where multiple requests are made to the server, if verbose
    #  is set to False, no messages will be displayed on the screen until the
    #  time indicated below has elapsed.  
    __MIN_SECS_NOT_VERBOSE = 60
    
    __NONE_AS_STR = ''
    
    
    def __init__(self, file_name: str='apikey.txt'):
        """
        Reads a valid api key from file_name

        Parameters
        ----------
        file_name : optional, the default is 'apikey.txt'.
            File where api key has been saved
        """
        with open(file_name) as f:
            self.__myapikey = f.readline()
        self.__querystring = {"api_key": self.__myapikey}
        self.__headers = {'cache-control': "no-cache"}
        self.__s = requests.Session()
        retries = Retry(total=AemetOpenData.__MAXREQUEST, 
                        backoff_factor=AemetOpenData.__BACKOFF_FACTOR, 
                        status_forcelist=[ AemetOpenData.__TOOMANYREQUESTS ])
        self.s.mount('http://', HTTPAdapter(max_retries=retries))


    @property
    def s(self):
        return self.__s
    

    def get_public_methods(self):
        return [name for name, _ in inspect.getmembers(self) \
                if not name.startswith('_')]


    @staticmethod 
    def file_names_in_dir(d_path: str, pattern: str) -> [str]:
        """
        Finds all the pathnames matching a specified pattern according to the
            rules used by the Unix shell

        Parameters
        ----------
        d_path (str). Directory path
        pattern (str): pattern

        Returns
        -------
        list of file names that matches pattern

        """
        d_path = pathlib.Path(d_path)
        if not d_path.exists() or not d_path.is_dir():
            msg = '{d_path} is not a directory'
            logging.append(msg)
            return []
        
        file_paths = d_path.glob(pattern)
        file_names = [f1.name for f1 in file_paths]
        return file_names


    @staticmethod
    def __file_name_clean(fname: str) -> str:
        """
        Replace some characters in a file name

        Parameters
        ----------
        fname (str). file name

        Returns
        -------
        File name

        """
        fname = fname.replace('-', '')
        fname = fname.replace(':', '')
        return fname


    @staticmethod
    def __swap_ts_limits(d1: Union[int, date, datetime], 
                         d2: Union[int, date, datetime]) -> ():
        if d1 > d2:
            tmp = d1
            d1 = d2
            d2 = tmp
            logging.append(f'{d1} - {d2} swapped')
        return (d1, d2)


    @staticmethod
    def __shrink_ts_limits(d1: Union[int, date, datetime], 
                           d2: Union[int, date, datetime]) -> ():
        """
        If the year of the lower date  is less than LOWEST_YEAR changes the 
            year to LOWEST_YEAR. If the year of the upper date is greater than
            the current year, changes it to this year
        Parameters
        ----------
        d1 : Lower limit of a time series
        d2 : Upper limit of a time series

        Returns
        -------
        (d1, d2)
        """
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
    def __daily_ranges_get_check_types(d1, d2, selected_stations) -> bool:
        if type(d1) != type(d2):
            logging.append('d1 and d2 must have the same type')
            return False

        if not isinstance(d1, (date, datetime)):
            logging.append('d1 and d2 must be of type date or datetime')
            return False

        if not isinstance(selected_stations, bool):
            logging.append('selected_stations must be of type bool')
            return False
        return True


    @staticmethod
    def daily_ranges_get(d1: Union[date, datetime], 
                         d2: Union[date, datetime], 
                         selected_stations: bool = True) \
        -> [[str, str]]:
        """
        Let d1 and d2 be the lower and upper limits of a daily time series. It 
            generates a list of subperiods of equal length (except the last
            one) each characterized by its lower and upper limit.
        Parameters
        ----------
        d1 : First day of the time series
        d2 : Last day of the time series
        selected_stations : If True each subperiod has a length of  
            __MAX_NDAYS_STATION; else __MAX_NDAYS_ALL_STATIONS
        Returns
        -------
        A list of limits of the subperiods; each element in the list is a
            tuple of 2 elements: The first day of the subperiod and the last 
            day of the subperiod, in the format required by Aemet OpenData 
        """
        if not AemetOpenData.__daily_ranges_get_check_types\
            (d1, d2, selected_stations):
            return []
        
        d1, d2 = AemetOpenData.__shrink_ts_limits(d1, d2)
        
        if selected_stations:
            max_ndays = timedelta(AemetOpenData.__MAX_NDAYS_STATION)
        else:
            max_ndays = timedelta(AemetOpenData.__MAX_NDAYS_ALL_STATIONS)
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
                days_range.append([scd, sdu])
            else:
                du = cd + diff
                sdu = du.strftime('%Y-%m-%dT23:59:59UTC')
                days_range.append([scd, sdu])
                break
            cd = du + one_day
            if cd > today:
                break
        return days_range


    @staticmethod
    def __years_ranges_get_check_parameters(y1, y2) -> bool:
        if type(y1) != type(y2):
            logging.append('y1 and y2 must have the same type')
            return False
        
        if not isinstance(y1, (int, date, datetime)):
            logging.append('y1 and y2 must be of type int or date or datetime')
            return False
        return True

        
    @staticmethod
    def years_ranges_get(y1: Union[int, date, datetime], 
                         y2: Union[int, date, datetime]) -> [[str, str]]:
        """
        Let d1 and d2 be the lower and upper limits of a time series with 
            yearly discretization. It generates a list of subperiods of equal
            length (except the last one) each characterized by its lower and
            upper limit.

        Parameters
        ----------
        d1 : First day of the period (date or datimetime types) or first year
            of the period (int type)
        d2 : Last day of the period (date or datimetime types) or last year
            of the period (int type)
        Returns
        -------
        A list of limits of the subperiods; each element in the list is a
            tuple of 2 elements: The first year of the subperiod and the last 
            year of the subperiod, in the format required by Aemet OpenData 
        """
        if not AemetOpenData.__years_ranges_get_check_parameters(y1, y2):
            return []
        
        if isinstance(y1, (date, datetime)):
            y1 = y1.year
            y2 = y2.year
        
        y1, y2 = AemetOpenData.__shrink_ts_limits(y1, y2)
        
        cy = y1
        max_years_station = AemetOpenData.__MAX_NYEARS_STATION
        years_range = []
        for i in range(5000):
            diff = y2 - cy
            scy = str(cy)
            if diff > max_years_station:
                yu = cy + max_years_station
                syu = str(yu)
                years_range.append([scy, syu])
            else:
                yu = cy + diff
                syu = str(yu)
                years_range.append([scy, syu])
                break
            cy = yu + 1
        return years_range


    def __request_get(self, url: str) -> ():
        """
        It makes a get request to Aemet urls
        Parameters
        ----------
        url : url.
            
        Raises
        ------
        SystemExit

        Returns
        -------
        A tuple with the following content:
        1) Requests.Response.status.code
        2) Requests.Response.reason
        3) description : A specific Aemet description in the response
        4) data : Data response in json format. 
            Depending on url the response can be:
            1. First request to the server.
            1.1. A dictionary with scalar values.
            2. Second request to the server.
            2.1. Metadata of the meteorological data. A dictionary with scalar
                 values except for the key 'campos', which is a list of
                 dictionaries; each dictionary in the list is the description
                 of a type of meteorological data.
            2.1. Metadata of weather station characteristics. A dictionary
                 of scalar values
            2.2. Station feature data. A dictionary of scalar values.
            2.3. Meteorological data. A list of dictionaries. Each dictionary
                 in a row of data from a station on a date. 

        """

        try:
            r = self.s.get\
                (url, headers=self.__headers, params=self.__querystring,
                 timeout=AemetOpenData.__TIMEOUT)
            r.raise_for_status()
            
            decoded_content = r.content.decode('ISO-8859-15')
            data = json.loads(decoded_content)
            description = data['descripcion'] if 'descripcion' \
                in data else ''            
            return (r.status_code, r.reason, description, data)
        except requests.exceptions.HTTPError as err:
            msg = f'HTTPError, code {r.status_code}: {r.reason}'
            logging.append(msg)
            raise SystemExit(err)
        except requests.exceptions.JSONDecodeError as err:
            msg=f'JSON decode error {err}'  
            logging.append(msg)
            raise SystemExit(err)             
        except Exception as err:
            msg = f'Request error {err}'
            logging.append(msg)
            raise SystemExit(err)        


    def __data_download_status\
        (self, file_names: {}, saved_files: [str], verbose: bool) -> \
            {str: bool, str: bool}:
        """
        Determines whether a data request will be made to the server. For this
            purpose it compares the name of the files where the data to be
            downloaded from the server will be stored (2 names, one for the
            data and one for the metadata) with a list of files with 
            previously downloaded data; if the file already exists the data
            will not be downloaded again.

        Parameters
        ----------
        file_names : It is a dictionary with 2 keys: data and metadata. 
            Each key contains the name of the file where the respective data
            to be downloaded will be saved and then stored with these
            file names
        saved_files : Names of previously saved files
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.
        Returns
        -------
        Dictionary with 2 keys: data and metadata
            For each key, if it has the value True, the data request will be
            performed.

        """
        request_data = {'data': True, 'metadata': True}
        if saved_files:
            for k, v in file_names.items():
                if v in saved_files:
                    msg = f'{v} has been previously downloaded'
                    logging.append(msg, verbose)
                    request_data[k] = False
        return request_data


    @staticmethod
    def __get_dict_template(ld: [{}]) -> {}:
        """
        Extracts a ordered list of unique keys from ls. Then it sets
            a dictionary with these unique keys with a default unique scalar
            value 

        Parameters
        ----------
        ld : [{}], Each dict has its own list of keys, some are common 
            between dictionaries

        Returns
        -------
        new_dict : Dictionary with keys in ls and a scalar defaulto value

        """
        
        default_value = AemetOpenData.__NONE_AS_STR
        unique_keys = set()
        
        for d in ld:
            unique_keys.update(d.keys())
        
        unique_key_list = list(unique_keys)
        unique_key_list.sort()
        
        new_dict = {key: default_value for key in unique_key_list}
        return new_dict


    @staticmethod
    def ldicts_2_ltuples_eq_len(dict_template: {}, data:[{}]):
        """
        Given a list of dicts and each dict in the list has its own keys,
            some of them can be common between dictionaries, it yields tuples
            of eq length with a default value __NONE_AS_STR instead of
            None

        Parameters
        ----------
        dict_template : dict with all keys in data
        data : list of dicts with data

        Yields
        ------
        values in aech dict of data with null values as a default value

        """
        for row in data:
            base = dict_template.copy()
            for k, v in row.items():
                base[k] = v
            yield tuple(base.values())


    def __save_to_csv\
        (self, csv_file_path: str, data: Union[dict, __list_dict],
         mold: {}= {}) -> None:
        """
        Save to a csv file the response to a request to the Aemet server 
            previously converted to json format. The response can be a list
            of dictionaries or a dictionary. In case metadata has been
            requested, the meaning of the data columns are provided 
            in the key 'campos'; data['campos'] is a list of dictionaries;
            in this case data['campos'] is the only content that is saved.

        Parameters
        ----------
        csv_file_path : csv file path where data will be saved
        data : json response as a list od dictionaries. Not all the 
            dictionaries have the same keys
        Raises
        ------
        ValueError
        Returns
        -------
        None

        """
        def dict_template_get(list_of_dicts):
            default_value = ''
            unique_keys = set()
            for dict1 in list_of_dicts:
                unique_keys.update(dict1.keys())
            unique_keys_list = sorted(list(unique_keys))
            dict_template = {key: default_value for key in unique_keys_list}
            return dict_template 


        if not isinstance(data, (list, dict)):
            msg = 'data must have type list or dict'
            raise ValueError(msg)
        
        if not data:
            return
            
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            csvwriter = csv.writer(csvfile)
        
            if isinstance(data, list):
                dict_template = self.__get_dict_template(data)
                new_data = [row for row in \
                    self.ldicts_2_ltuples_eq_len(dict_template, data)]
                
                header = dict_template.keys()
                csvwriter.writerow(header)
                csvwriter.writerows(new_data)
            else:
                if 'campos' in data:
                    dict_template = dict_template_get(data['campos'])
                    new_data = [row for row in \
                        self.ldicts_2_ltuples_eq_len(dict_template, 
                                                     data['campos'])]
                    header = dict_template.keys()
                    csvwriter.writerow(header)
                    csvwriter.writerows(new_data)
                else:
                    header = data.keys()
                    csvwriter.writerow(header)
                    for item in data:
                        csvwriter.writerow(item.values())


    def __request_and_save_file\
        (self, url: str, k: str, ofile_names: dict, dir_path, verbose: bool,
         start_time: ScalarContainer) -> bool:
        """
        It makes a first data request to the server. If the response is ok it
            makes a second request using one of the two urls supplied in the
            first response (one url for the data and one for the metadata).
            The url used in the second request is determined by the value of
            the k parameter. 

        Parameters
        ----------
        url : A valid url
        k : A string with value 'data' or 'metadata'
        ofile_names : Names for the output files. It's a dict with 2 keys:
            'data' and 'metadata'
        dir_path : Directory path where file with downloaded data will be saved
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.
        start_time : It is a ScalarContainer object that stores a reference
            time that is used to display the messages on the screen or not
            according to the value of the verbose parameter

        Returns
        -------
        True if the process is completed, otherwise False

        """
        status_code1, reason1, description1, data1 = \
            self.__request_get(url)
        if status_code1 != AemetOpenData.__RESPONSEOK:
            return False
        
        aemet_key = 'datos' if k == 'data' else 'metadatos'
        
        if aemet_key in data1:
            status_code2, reason2, description2, data2 = \
                self.__request_get(data1[aemet_key])
            if status_code2 != AemetOpenData.__RESPONSEOK:
                return False
            
            file_path = dir_path.joinpath(ofile_names[k])
            self.__save_to_csv(file_path, data2)
            
            file_name = pathlib.Path(file_path).name
            msg = f'{file_name}: {status_code2}, {reason2} {description2}'
            if verbose:
                logging.append(msg)
            else:
                xtime = time() - start_time.x
                if xtime > AemetOpenData.__MIN_SECS_NOT_VERBOSE:
                    logging.append(msg)
                    start_time.x = time()
            
            return True
        else:
            return False


    @staticmethod
    def __check_fecth_value(fetch: str) -> bool:
        """
        Checks if fetch has a valid value

        Parameters
        ----------
        fetch : content to validate 

        Returns
        -------
        True if fetch has a valid value, False otherwise

        """
        VALID_FETCH_VALUES = ('data', 'metadata', 'both')
        fetch = fetch.lower()
        if fetch not in VALID_FETCH_VALUES:
            msg = ', '.join(VALID_FETCH_VALUES)
            logging.append(f'fetch not in {msg}')
            return False
        else:
            return True


    def __request_meteo_data_all_stations\
        (self, dr: [(str, str)], dir_path: str, fetch: str='both',
         use_files: bool=True, verbose: bool=False) -> [str]: 
        """
        Makes one or many request to the server and downloads the data 
            in one or many files depending on the range of time series 
            requested, in relation to the limits defined by Aemet for each
            request. Daily data of all stations are returned
        It returns the names of the csv files where data will be saved as an 
            iterator using yield
        Parameters
        ----------
        dr : date or year ranges to do valid requests
        fetch: str in ('data', 'metadata', 'both')
        dir_path. Directory where csv files will be saved
        use_files : If True checks that the file name already exists in 
            dir_path and does not make the request to the server; otherwise 
            the request is made and the pre-existing file is overwritten.
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.
        Raises
        ------
        ValueError
        Returns
        -------
        List with the name of the csv file where data has been saved
        """
        
        """
        If you modify the pattern of ofile_names, you must review the regex
            patterns in concatenate_files method
        """

        if use_files:
            saved_files = AemetOpenData.file_names_in_dir(dir_path, '*.csv')
        else:
            saved_files = []

        dir_path = pathlib.Path(dir_path)            

        url_template =  'https://opendata.aemet.es/opendata/api/valores/'+\
            'climatologicos/diarios/datos/fechaini/{}/fechafin/{}/'+\
                'todasestaciones'
        
        file_name_template = 'stations_{}_{}_{}.csv'
        
        start_time = ScalarContainer(time())
        downloaded_files = [] 
        for dr1 in dr:
            
            url = url_template.format(dr1[0], dr1[1])

            ofile_names = AemetOpenData.__set_output_file_names_with_template\
                (fetch, file_name_template, dr1, True)

            dd_status = self.__data_download_status\
                (ofile_names, saved_files, verbose)
            for k, v in dd_status.items():
                if v == False:
                    continue
    
                if not self.__request_and_save_file\
                    (url, k, ofile_names, dir_path, verbose, start_time):
                    continue
            
                downloaded_files.append(ofile_names[k])
                
        return downloaded_files
        

    @staticmethod 
    def __meteo_data_all_stations_check_type_parameters\
        (d1, d2, dir_path, fetch, verbose, use_files):
        """
        Check the parameter types of method meteo_data_all_stations
        
        Returns
        -------
        bool: True if parameters have the right types
        """
            
        params = {'dir_path': dir_path, 'fetch': fetch}
        if not AemetOpenData.__check_params_type(params, str):
            return False
 
        if type(d1) != type(d2):
            logging.append('types for d1 and d2 must be equal')
            return False
        if not isinstance(d1, (date, datetime)):
            logging.append('Incorrect type for d1 and d2')
            return False
        
        params = {'verbose': verbose, 'use_files': use_files}
        if not AemetOpenData.__check_params_type(params, bool):
            return False

        return True


    def meteo_data_all_stations\
        (self, d1: date, d2: date, dir_path: str, fetch: str='both',
         verbose: bool=True, use_files: bool=True) -> [str]:
        """
        Retrieves daily meteorological data from all the station in Aemet
            OpenData server.

        Parameters
        ----------
        d1 : Initial date
        d2 : Final date
        dir_path : Each request to Aemet server is saved as a csv file in the
            directory dir_path      
        fetch: str in ('data', 'metadata', 'both')       
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.
        use_files : If True checks that the file name already exists in dir_path
            and does not make the request to the server; otherwise the request
            is made and the pre-existing file is overwritten.
        Returns
        -------
        [] with the the names of saved csv files. Each file has a subset of
            all data downloaded
        """

        if not AemetOpenData.__meteo_data_all_stations_check_type_parameters\
            (d1, d2, dir_path, fetch, verbose, use_files):
            return []

        if not AemetOpenData.__check_fecth_value(fetch):
            return []

        d1, d2 = AemetOpenData.__shrink_ts_limits(d1, d2)
            
        dir_path = pathlib.Path(dir_path)
        if not dir_path.exists() or not dir_path.is_dir():
            msg = '{dir_path} is not a directory'
            logging.append(msg)
            return []

        dr = AemetOpenData.daily_ranges_get(d1, d2, selected_stations=False)
        
        downloaded_files = self.__request_meteo_data_all_stations\
            (dr, dir_path, fetch, use_files, verbose)

        return downloaded_files


    @staticmethod
    def __set_output_file_names_with_template\
        (fetch: str, file_name_template: str, params: [str],
         label_data_meta: bool) -> dict:
        """
        Two file names are generated for each data request to the Aemet server: 
            the first name is for the data file and the second for the
            metadata file.

        Parameters
        ----------
        fetch : Must have the value of 'data', 'metadata' or 'both'. 
            Its value is checked in the calling function
        file_name_template : Template for file names
        parameters : Parameters for placeholders in file_name_template
        label_data_meta : 'data' or 'metadata' label will be added to 
            file_name_template
        Returns
        -------
        Dictionary {'data': data file name, 'metadata': metadata file name}

        """
        if len(file_name_template) == 0:
            return file_name_template
    
        placeholders = file_name_template.count('{}')
        n = len(params)
        if label_data_meta:
            if placeholders != n + 1:
                msg = 'num. placeholders should be equal num. params + 1'
                raise ValueError(msg)
        else:
            if placeholders != n:
                msg = 'num. placeholders should be equal num. params'
                raise ValueError(msg)
                
        file_names = {'data': None, 'metadata': None}
        if fetch in ('data', 'both'):
            _params = params.copy()
            if label_data_meta: _params.append('data') 
            file_names['data'] = file_name_template.format(*_params)
        
        if fetch in ('metadata', 'both'):
            _params = params.copy()
            if label_data_meta: _params.append('metadata')
            file_names['metadata'] = file_name_template.format(*_params)

        for k, v in file_names.items():
            if v is not None:
                file_names[k] = AemetOpenData.__file_name_clean(v)

        return file_names


    def meteo_stations(self, dir_path: str, fetch: str='both',
             use_files: bool=True, verbose: bool=True) -> None:
        """
        Retrieves characteristics of meteorological stations in Aemet
            OpenData and saves them to a file

        Parameters
        ----------
        fetch: str in ('data', 'metadata', 'both')
        dir_path. Directory path where csv files will be saved
        use_files : If True checks that the file name already exists in 
            dir_path and does not make the request to the server; otherwise 
            the request is made and the pre-existing file is overwritten.
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.

        Returns
        -------
        yields file name of data, metadata or both
        """
        
        url = 'https://opendata.aemet.es/opendata/api/valores/'+\
            'climatologicos/inventarioestaciones/todasestaciones'

        dir_path = pathlib.Path(dir_path)
        if not dir_path.exists() or not dir_path.is_dir():
            msg = '{dir_path} is not a directory'
            logging.append(msg)
            return []

        if not AemetOpenData.__check_fecth_value(fetch):
            return

        if use_files:
            saved_files = AemetOpenData.file_names_in_dir(dir_path, '*.csv')
        else:
            saved_files = []
        
        file_name_template = 'estaciones_open_data_{}.csv'
        
        start_time = ScalarContainer(time())

        ofile_names = AemetOpenData.__set_output_file_names_with_template\
            (fetch, file_name_template, [], True)

        dd_status = self.__data_download_status\
            (ofile_names, saved_files, verbose)
        for k, v in dd_status.items():
            if v == False:
                continue

            if not self.__request_and_save_file\
                (url, k, ofile_names, dir_path, verbose, start_time):
                continue


    @staticmethod
    def __check_time_step_value(time_step: str) -> bool:
        """
        Checks if the value of time_step is one of the valid values

        Parameters
        ----------
        time_step : str to check its value

        Returns
        -------
        True if time_step has a valid value, False otherwise.
        """
        VALID_TIME_STEPS = ('day', 'month')
        if time_step not in VALID_TIME_STEPS:
            msg = ', '.join(VALID_TIME_STEPS)
            logging.append(f'time_step not in {msg}')
            return False
        return True                    


    @staticmethod 
    def __check_params_type(params: dict, expected_type: type) -> bool:
        """
        Checks if the type of the parameters are of the expected type

        Parameters
        ----------
        params : Pairs key name of the parameter, value or the parameter 
        expected_type : type to check

        Returns
        -------
        True if params are of the expected type, otherwise False
        """
        invalid_type = []
        invalid_sequence = []
        
        for key, value in params.items():
            if isinstance(value, (list, tuple)):
                elements = []
                for i, item in enumerate(value):
                    if not isinstance(item, expected_type):
                        elements.append(i)
                if elements:
                    invalid_sequence.append(key)
            else:    
                if not isinstance(value, expected_type):
                    invalid_type.append(key)
    
        if invalid_type:
            invalid_params = ', '.join(invalid_type)
            logging.append('These parameters must be of type' +\
                           f'{expected_type.__name__}: {invalid_params}')
        if invalid_sequence:
            invalid_params = ', '.join(invalid_sequence)
            logging.append('These sequences must have all its elements' + \
                           ' of type' +\
                           f'{expected_type.__name__}: {invalid_params}')
        if invalid_type or invalid_sequence:
            return False
        return True
        

    @staticmethod 
    def __meteo_data_by_station_check_type_parameters\
        (time_step, d1, d2, stations, dir_path, fetch, verbose, use_files) \
            -> bool:
        """
        Check the parameter types of method meteo_data_by_station
        
        Returns
        -------
        True if all parameters have the expected types, False otherwise
        """
        params = {'time_step': time_step, 'stations': stations, 
                  'dir_path': dir_path, 'fetch': fetch}
        if not AemetOpenData.__check_params_type(params, str):
            return False
 
        if type(d1) != type(d2):
            logging.append('types for d1 and d2 must be equal')
            return False
        
        if not isinstance(d1, (int, date, datetime)):
            logging.append('Incorrect type for d1 and d2')
            return False
        
        params = {'verbose': verbose, 'use_files': use_files}
        if not AemetOpenData.__check_params_type(params, bool):
            return False

        return True


    def meteo_data_by_station\
        (self, time_step: str, d1: date, d2: date, 
         stations: Union[__TupStr, __LisStr, str],
         dir_path: str, fetch: str='both',
         verbose: bool=True, use_files: bool=True) -> [str]:
        """
        Retrieves daily or monthly meteorological data from Aemet OpenData
            server station by station.

        Parameters
        ----------
        time_step : data to download can have a temporal resolution of
            'day' or 'month'
        d1 : Initial date (daily data) or year (monthly data)
        d2 : Final date or year. Equal d1
        stations : List of stations or only one station as str
        dir_path : Directory path where files will be saved
        fetch: str in ('data', 'metadata', 'both')
        verbose: If True, all possible messages are displayed on the screen;
            if False, fewer messages are displayed.
        use_files : If True checks that the file name already exists in dir_out
            and does not make the request to the server; otherwise the request
            is made and the pre-existing file is overwritten.
        Returns
        -------
        [] with the the names of saved csv files. Each file has a subset of
            all data downloaded
        """

        if not AemetOpenData.__meteo_data_by_station_check_type_parameters\
            (time_step, d1, d2, stations, dir_path, fetch, verbose, use_files):
            return []

        AemetOpenData.__check_time_step_value(time_step)

        d1, d2 = AemetOpenData.__shrink_ts_limits(d1, d2)
        
        if isinstance(stations, str):
            stations = (stations,)
            
        dir_path = pathlib.Path(dir_path)
        if not dir_path.exists() or not dir_path.is_dir():
            msg = '{dir_path} is not a directory'
            logging.append(msg)
            return []
        
        if not AemetOpenData.__check_time_step_value(time_step):
            return []
        
        if time_step == 'day':
            time_periods = AemetOpenData.daily_ranges_get(d1, d2)
        else:
            time_periods = AemetOpenData.years_ranges_get(d1, d2)
        
        if not time_periods:
            logging.append('No elements in time_periods')
            return []

        if not AemetOpenData.__check_fecth_value(fetch):
            return []

        if time_step == 'day':
            url_template = 'https://opendata.aemet.es/opendata/api/valores/'+\
                'climatologicos/diarios/datos/fechaini/'+\
                    '{}/fechafin/{}/estacion/{}'
        else:
            url_template = 'https://opendata.aemet.es/opendata/api/valores/'+\
                'climatologicos/mensualesanuales/datos/anioini/'+\
                    '{}/aniofin/{}/estacion/{}'

        if use_files:
            saved_files = AemetOpenData.file_names_in_dir(dir_path, '*.csv')
        else:
            saved_files = []
        
        file_name_template = '{}_{}_{}_{}.csv'

        downloaded_files = []
        start_time = ScalarContainer(time())
        for station1 in stations:
        
            for tp1 in time_periods:
                
                url = url_template.format(tp1[0], tp1[1], station1)
    
                params = [station1] + tp1
                ofile_names = \
                    AemetOpenData.__set_output_file_names_with_template\
                    (fetch, file_name_template, params, True)
    
                dd_status = self.__data_download_status\
                    (ofile_names, saved_files, verbose)
                for k, v in dd_status.items():
                    if v == False:
                        continue
        
                    if not self.__request_and_save_file\
                        (url, k, ofile_names, dir_path, verbose, start_time):
                        continue
                
                    downloaded_files.append(ofile_names[k])
        
        return downloaded_files
