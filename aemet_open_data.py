# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 10:15:17 2019

@author: solis
"""
try:
    from datetime import date, datetime, timedelta
    import pandas as pd
    import requests
    from time import time
    
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

    _RESPONSEOK = 200
    _UNAUTHORIZED = 401
    _NOTFOUND = 404
    _TOOMANYREQUESTS = 429
    _MAXREQUEST = 3
    
    _SLEEPSECONDS = 15
    _MAX_NDAYS_ESTACION = 365*5
    _MAX_NYEARS_ESTACION = 3
    
    
    def __init__(self):
        # Leo mi apikey desde fichero
        with open('apikey.txt') as f:
            self.myapikey = f.readline()
        self.querystring = {"api_key": self.myapikey}
        self.headers = {'cache-control': "no-cache"}


    @staticmethod
    def daily_ranges_get(d1: date, d2: date) -> ((str, str)):
        """
        Genera una serie de rangos de fecha days_range entre d1 y d2 con una 
        duración máxima max_ndays; las fechas en days_range son str en formato
        de la API de Aemet OpenData
        """
        if d1 > d2:
            tmp = d1
            d1 = d2
            d2 = tmp
            logging.append(f'{d1} - {d2} swapped')
    
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
    def years_ranges_get(y1: int, y2: int) -> ((str, str)):
        """
        Genera una serie de rangos de años years_range entre y1 y y2 con una 
        duración máxima max_nyears; los años en years_range son str de 4
        dígitos
        """
        if y1 > y2:
            tmp = y1
            y1 = y2
            y2 = tmp
            logging.append(f'{y1:d} - {y2:d} swapped')
        
        if y1 < 1875:
            y1 = 1875
            logging.append('y1 is setted to 1875')

        if y2 > date.today().year:
            y2 = date.today().year
            logging.append(f'y2 is setted to {date.today().year:d}')
    
        max_nyears = AemetOpenData._MAX_NYEARS_ESTACION
        cy = y1
        years_range = []
        for i in range(5000):
            diff = y2 - cy
            scy = str(cy)
            if diff > max_nyears:
                yu = cy + max_nyears
                syu = str(yu)
                years_range.append((scy, syu))
            else:
                yu = cy + diff
                syu = str(yu)
                years_range.append((scy, syu))
                break
            cy = yu + 1
        return years_range


    def __request(self, url: str, metadata: bool=False):
        """
        Petición de datos única (serie no temporal)

        Parameters
        ----------
        url : url
        metadata: If True only metadata are downloaded, otherwse only data
        are downloaded

        Returns
        -------
        df : data, metadata or None
        """

        df = None
        non_stop = True
        while non_stop: 
            response = requests.request("GET", url, 
                                        headers=self.headers,
                                        params=self.querystring,
                                        timeout=(2, 5))

            if response.status_code == AemetOpenData._RESPONSEOK:
                content = response.json()
                if metadata:
                    field = 'metadatos'
                else:
                    field = 'datos'
                if field in content:
                    df = pd.read_json(content[field],
                                      encoding='latin-1')
                msg = f'{content["estado"]}, {content["descripcion"]}'
                logging.append(msg) 
                non_stop = False
            elif response.status_code == AemetOpenData._TOOMANYREQUESTS:
                time.sleep(AemetOpenData._SLEEPSECONDS)
            elif response.status_code in (AemetOpenData._UNAUTHORIZED,
                                          AemetOpenData._NOTFOUND):
                content = response.json()
                msg = f'{content["estado"]}, {content["descripcion"]}'
                logging.append(msg) 
                non_stop = False                        
            else:
                msg = f'status code {response.status_code}'
                logging.append(msg)                                                                                
                non_stop = False
        return df


    def __request_variables_climatologicas(self, dr: [(str, str)],
                                           estaciones: (str), 
                                           mtype: str, 
                                           metadata: bool) -> pd.DataFrame: 
        """
        Hace la petición de datos a Aemet OpenData
        Parameters
        ----------
        dr : ver funciones de llamada (valid_mtypes)
        estaciones: tupla de códigos de estaciones
        mtype : nombre de la función de llamada (valid_mtypes)
        metadata: If True only metadata are downloaded, otherwse only data
        are downloaded
        """
        valid_mtypes = ('climatologias_diarias', 'climatologias_mensuales')
        if mtype not in valid_mtypes:
            a = ', '.join(valid_mtypes)
            raise ValueError(f'mtype not in {a}')
        
        dfs = []
        for e1 in estaciones:
            for dr1 in dr:
                if mtype == 'climatologias_diarias':
                    url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{dr1[0]}/fechafin/{dr1[1]}/estacion/{e1}'
                else:
                    url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/anioini/{dr1[0]}/aniofin/{dr1[1]}/estacion/{e1}'
                    
                non_stop = True
                while non_stop: 
                    response = requests.request("GET", url, 
                                                headers=self.headers,
                                                params=self.querystring,
                                                timeout=(2, 5))

                    if response.status_code == AemetOpenData._RESPONSEOK:
                        content = response.json()
                        if metadata:
                            field = 'metadatos'
                        else:
                            field = 'datos'
                        if field in content:
                            df = pd.read_json(content[field],
                                              encoding='latin-1')
                            dfs.append(df)
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: '+\
                            f'{content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False
                        if metadata and len(dfs)>0:
                            return dfs[0]
                    elif response.status_code == AemetOpenData._TOOMANYREQUESTS:
                        time.sleep(AemetOpenData._SLEEPSECONDS)
                    elif response.status_code in (AemetOpenData._UNAUTHORIZED,
                                                  AemetOpenData._NOTFOUND):
                        content = response.json()
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: '+\
                            f'{content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False                        
                    else:
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: '+\
                            f'status code {response.status_code}'
                        logging.append(msg)                                                                                
                        non_stop = False
        if dfs:
            return pd.concat(dfs, axis=0, ignore_index=True)
        else:
            return dfs


    def climatologias_estaciones(self, metadata: bool=False) -> pd.DataFrame:
        """
        Devuelve los datos de las estaciones con datos de variables
        climatológicas en Aemet OpenData

        Parameters
        ----------
        metadata: If True only metadata are downloaded, otherwse only data
        are downloaded

        Returns
        -------
        data, metadata or None
        """
        url = 'https://opendata.aemet.es/opendata/api/valores/climatologicos/inventarioestaciones/todasestaciones'
        data = self.__request(url, metadata)       
        return data


    def climatologias_diarias(self, d1: date, d2: date, estaciones:(str),
                                metadata: bool=False) -> pd.DataFrame:
        """
        Devuelve una dataframe con los datos climatológicos diarios de 
        una o varias estaciones en un rango de fechas

        Parameters
        ----------
        d1 : fecha inicial
        d2 : fecha final
        estaciones : Lista de estaciones o una estación como str
        metadata: If True only metadata are downloaded, otherwse only data
        are downloaded

        Returns
        -------
        data, metadata or None
        """

        if isinstance(estaciones, str):
            estaciones = (estaciones,)
        
        dr = AemetOpenData.daily_ranges_get(d1, d2)

        mtype = 'climatologias_diarias'
        data = self.__request_variables_climatologicas(dr, estaciones, mtype,
                                                       metadata)
        return data


    def climatologias_mensuales(self, y1: (int, date, datetime), 
                                y2: (int, date, datetime), 
                                estaciones:(str, [str]),
                                metadata: bool=False) -> pd.DataFrame:
        """
        Devuelve una dataframe con los datos climatológicos mensuales de 
        una o varias estaciones en un rango de años

        Parameters
        ----------
        y1 : año inicial
        y2 : año final
        estaciones : Lista de estaciones o una estación como str
        metadata: If True only metadata are downloaded, otherwse only data
        are downloaded
        
        Returns
        -------
        data or metadata
        """

        if isinstance(estaciones, str):
            estaciones = (estaciones,)
        
        if isinstance(y1, (date, datetime)):
            y1 = y1.year
        
        if isinstance(y2, (date, datetime)):
            y2 = y2.year
            
        dr = AemetOpenData.years_ranges_get(y1, y2)
        
        mtype = 'climatologias_mensuales'
        data = self.__request_variables_climatologicas(dr, estaciones, mtype,
                                                       metadata)
        return data
