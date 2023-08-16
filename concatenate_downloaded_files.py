# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 11:19:26 2023

@author: solis
"""
import csv
import pathlib
import re
import sqlite3

import littleLogging as logging


class Concatenate_downloaded_files():

        
    __FILE_PATTERNS = \
        {'stations_day': r'^stations(_\d{8}T\d{6}UTC){2}_data\.csv$',
         'station1_day': r'^(?!stations)(.{1,})(_\d{8}T\d{6}UTC){2}_data\.csv$',
         'station1_month': r'^(?!stations)(.{1,})(_\d{4}){2}_data\.csv$',
         }
    __DBNAME = \
        {'stations_day': 'met_all_stations',
         'station1_day': 'met_day_selected_stations',
         'station1_month': 'met_month_selected_stations',
         }        

    
    def __init__(self):
        self.dbname = None

            
    def concat(self, d_path: str, file_type: str) :
        
        dir_path = self.exists_dir_path(d_path)
        
        par = {'file_type': file_type}
        self.__valid_value(par, self.__FILE_PATTERNS.keys())
        
        f_paths = self.__get_file_paths(dir_path, file_type)
        
        headers = self.__get_unique_ordered_headers(f_paths)
        
        if not self.__create_table(dir_path, file_type, headers):
            return False


    def exists_dir_path(self, d_path: str) -> pathlib.Path:
        directory_path = pathlib.Path(d_path)
        if not directory_path.exists() or not directory_path.is_dir():
            msg = f"Doesn't exists {d_path}"
            logging.append(msg)
            raise ValueError(msg)
        return directory_path


    def __valid_value(self, param : {}, valid_values : []):
        first_item = next(iter(param.items()))

        key, value = first_item
        if value not in valid_values:
            a = ', '.join(valid_values)
            msg = f'{key} must have a value in {a}'
            logging.append(msg)
            raise ValueError(msg)


    def __get_file_paths(self, d_path: pathlib.Path, file_type: str) -> [] :
        f_paths = d_path.glob('*_data.csv')
        filtered_names = \
            [fp1 for fp1 in f_paths if \
             re.match(self.__FILE_PATTERNS[file_type], fp1.name)]
        return filtered_names


    def __get_unique_ordered_headers(self, file_paths: [str]) -> [str]:
        """
        Lets a list with CSV file paths, the function returns the headers in
            the files without repetitions and ordered by name

        Parameters
        ----------
        csv_file_paths : List with CSV file paths

        Returns
        -------
        sorted_headers

        """
        all_headers = set()
    
        for file_path in file_paths:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                headers = next(reader, None)
                if headers:
                    all_headers.update(headers)
    
        sorted_headers = sorted(all_headers)
        
        return sorted_headers


    def __create_table(self, d_path: pathlib.Path, file_type: str,
                       headers: [str]) -> bool:

        create_table_template = "create table {} ( {} );"
        column_template = "{} text"        
        
        dbname = self.__DBNAME[file_type]+'.db'
        dbpath = d_path.joinpath(dbname)
        if dbpath.exists() and dbpath.is_file():
            ans = input(f'\n{dbpath}\nalready exists, overwrite (y/n)?: ')
            if ans.lower() != 'y':
                return False

        columns = [column_template.format(h1) for h1 in headers]
        columns = ', '.join(columns)

        stm = create_table_template.format(dbpath, columns)
        
        conn = sqlite3.connect(dbpath)
        cur = conn.cursor()
        cur.execute(stm)
        conn.close()
        self.dbname = dbpath
        return True





