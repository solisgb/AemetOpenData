# -*- coding: utf-8 -*-
"""
Created on Sat Oct 26 10:15:17 2019

@author: solis
"""
try:
    from datetime import date, timedelta
    import json
    import pandas as pd
    import requests
    from time import time
    import traceback
    
    import littleLogging as logging
except ImportError as e:
    print( getattr(e, 'message', repr(e)))
    raise SystemExit(0)


class AemetOpenData():

    _RESPONSEOK = 200
    _UNAUTHORIZED = 401
    _NOTFOUND = 404
    _TOOMANYREQUESTS = 429
    _MAXREQUEST = 3
    
    _SLEEPSECONDS = 15
    _MAX_NDAYS_ESTACION = 365*5
    _MAX_NYEARS_ESTACION = 3
    
    
    def __init__(self):
        # Leo mi apikey
        with open('apikey.txt') as f:
            self.myapikey = f.readline()
        
        # Leo urls
        with open('urls.json', 'r') as f:
            self.urls = json.load(f)
        
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


    def __request_variables_climatologicas(self, dr: [(str, str)], mtype: str) -> pd.DataFrame: 
        """
        Hace la petición de datos a Aemet OpenData
        Parameters
        ----------
        dr : [(str, str)]
            DESCRIPTION.
        mtype : str
            DESCRIPTION.
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
                    url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/anioini/{dr[0]}/aniofin/{dr[1]}/estacion/{e1}'
                    
                non_stop = True
                while non_stop: 
                    response = requests.request("GET", url, 
                                                headers=self.headers,
                                                params=self.querystring,
                                                timeout=(2, 5))

                    if response.status_code == AemetOpenData._RESPONSEOK:
                        content = response.json()
                        if 'datos' in content:
                            df = pd.read_json(content["datos"], encoding='latin-1')
                            dfs.append(df) 
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False
                    elif response.status_code == AemetOpenData._TOOMANYREQUESTS:
                        time.sleep(AemetOpenData._SLEEPSECONDS)
                    elif response.status_code in (AemetOpenData._UNAUTHORIZED,
                                                  AemetOpenData._NOTFOUND):
                        content = response.json()
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False                        
                    else:
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: status code {response.status_code}'
                        logging.append(msg)                                                                                
                        non_stop = False
        if dfs:
            return pd.concat(dfs, axis=0, ignore_index=True)
        else:
            return dfs


    def climatologias_diarias(self, d1: date, d2: date, estaciones:(str)):
        """
        Devuelve una dataframe con los datos climatológicos diarios de 
        una o varias estaciones en un rango de fechas

        Parameters
        ----------
        d1 : fecha inicial
        d2 : fecha final
        estaciones : Lista de estaciones o una estación como str

        Returns
        -------
        dataframe con los datos
        """

        if isinstance(estaciones, str):
            estaciones = (estaciones,)
        
        dr = AemetOpenData.daily_ranges_get(d1, d2)

        mtype = 'climatologias_diarias'
        data = self.__request_variables_climatologicas(dr, mtype)
        return data

        
        # dfs = []
        # for e1 in estaciones:
        #     for dr1 in dr:
        #         url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/diarios/datos/fechaini/{dr1[0]}/fechafin/{dr1[1]}/estacion/{e1}'
        #         non_stop = True
        #         while non_stop: 
        #             response = requests.request("GET", url, 
        #                                         headers=self.headers,
        #                                         params=self.querystring,
        #                                         timeout=(2, 5))

        #             if response.status_code == AemetOpenData._RESPONSEOK:
        #                 content = response.json()
        #                 if 'datos' in content:
        #                     df = pd.read_json(content["datos"], encoding='latin-1')
        #                     dfs.append(df) 
        #                 msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
        #                 logging.append(msg) 
        #                 non_stop = False
        #             elif response.status_code == AemetOpenData._TOOMANYREQUESTS:
        #                 time.sleep(AemetOpenData._SLEEPSECONDS)
        #             elif response.status_code in (AemetOpenData._UNAUTHORIZED,
        #                                           AemetOpenData._NOTFOUND):
        #                 content = response.json()
        #                 msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
        #                 logging.append(msg) 
        #                 non_stop = False                        
        #             else:
        #                 msg = f'{e1}, {dr1[0]}, {dr1[1]}: status code {response.status_code}'
        #                 logging.append(msg)                                                                                
        #                 non_stop = False
        # if dfs:
        #     return pd.concat(dfs, axis=0, ignore_index=True)
        # else:
        #     return dfs


    def climatologias_mensuales(self, y1: int, y2: int, estaciones:(str)):
        """
        Devuelve una dataframe con los datos climatológicos mensuales de 
        una o varias estaciones en un rango de años

        Parameters
        ----------
        y1 : año inicial
        y2 : año final
        estaciones : Lista de estaciones o una estación como str

        Returns
        -------
        dataframe con los datos
        """
        
        # Una estación un rango de fechas de menos de 5 años
        url = self.urls['estacion_dia'][0]

        if isinstance(estaciones, str):
            estaciones = (estaciones,)
        
        dr = AemetOpenData.years_ranges_get(y1, y2)
        
        dfs = []
        for e1 in estaciones:
            for dr1 in dr:
                url = f'https://opendata.aemet.es/opendata/api/valores/climatologicos/mensualesanuales/datos/anioini/{dr[0]}/aniofin/{dr[1]}/estacion/{e1}'
                non_stop = True
                while non_stop: 
                    response = requests.request("GET", url, 
                                                headers=self.headers,
                                                params=self.querystring,
                                                timeout=(2, 5))

                    if response.status_code == AemetOpenData._RESPONSEOK:
                        content = response.json()
                        if 'datos' in content:
                            df = pd.read_json(content["datos"], encoding='latin-1')
                            dfs.append(df) 
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False
                    elif response.status_code == AemetOpenData._TOOMANYREQUESTS:
                        time.sleep(AemetOpenData._SLEEPSECONDS)
                    elif response.status_code in (AemetOpenData._UNAUTHORIZED,
                                                  AemetOpenData._NOTFOUND):
                        content = response.json()
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: {content["estado"]}, {content["descripcion"]}'
                        logging.append(msg) 
                        non_stop = False                        
                    else:
                        msg = f'{e1}, {dr1[0]}, {dr1[1]}: status code {response.status_code}'
                        logging.append(msg)                                                                                
                        non_stop = False
        if dfs:
            return pd.concat(dfs, axis=0, ignore_index=True)
        else:
            return dfs


if __name__ == "__main__":

    startTime = time()

    try:
         
        d1 = date(2000, 1, 1)
        d2 = date(2023, 1, 1)
        estaciones = ('7178I', '7002Y')
        
        aod = AemetOpenData()
        data = aod.climatologias_diarias(d1, d2, estaciones)
        print('Días descargados', len(data))

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

