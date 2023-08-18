# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 11:19:26 2023

@author: solis
"""
import csv
import pathlib
import os
import re
import sqlite3

import littleLogging as logging


class AOD_2db():


    __FILE_PATTERNS = \
        {'stations_day': r'^stations(_\d{8}T\d{6}UTC){2}_data\.csv$',
         'station1_day': r'^(?!stations)(.{1,})(_\d{8}T\d{6}UTC){2}_data\.csv$',
         'station1_month': r'^(?!stations)(.{1,})(_\d{4}){2}_data\.csv$',
         }
    __DBNAME = \
        {'stations_day': 'metd_all_stations.db',
         'station1_day': 'metd_selected_stations.db',
         'station1_month': 'metm_selected_stations.db',
         }        
    __DBTABLE = \
        {'stations_day': 'metd',
         'station1_day': 'metd',
         'station1_month': 'metm',
         }
    __MAX_ERRORS_2STOP_INSERTING = 3

    
    def __init__(self):
        # sqlite db
        self.dbpath: pathlib.Path = None
        self.table_name: str = None

            
    def to_db(self, d_path: str, file_type: str) :
        
        dir_path = AOD_2db.exists_dir_path(d_path)
        
        par = {'file_type': file_type}
        AOD_2db.__valid_value(par, self.__FILE_PATTERNS.keys())
        
        f_paths = AOD_2db.__get_file_paths(dir_path, file_type)
        
        headers = AOD_2db.__get_unique_ordered_headers(f_paths)
        
        if not self.__create_table(dir_path, file_type, headers):
            return False
        
        if not self.__insert_data(f_paths):
            return False


    @staticmethod
    def exists_dir_path(d_path: str) -> pathlib.Path:
        directory_path = pathlib.Path(d_path)
        if not directory_path.exists() or not directory_path.is_dir():
            msg = f"Doesn't exists {d_path}"
            logging.append(msg)
            raise ValueError(msg)
        return directory_path


    @staticmethod
    def __valid_value(param : {}, valid_values : []):
        first_item = next(iter(param.items()))

        key, value = first_item
        if value not in valid_values:
            a = ', '.join(valid_values)
            msg = f'{key} must have a value in {a}'
            logging.append(msg)
            raise ValueError(msg)


    @staticmethod
    def __get_file_paths(d_path: pathlib.Path, file_type: str) -> [] :
        f_paths = d_path.glob('*_data.csv')
        filtered_names = \
            [fp1 for fp1 in f_paths if \
             re.match(AOD_2db.__FILE_PATTERNS[file_type], fp1.name)]
        return filtered_names


    @staticmethod
    def __get_unique_ordered_headers(file_paths: [str]) -> [str]:
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


    @staticmethod
    def __get_dbpath(d_path: pathlib.Path, dbname: str) -> pathlib.Path:

        if not pathlib.Path(d_path).is_absolute():
            cwd = os.getcwd()
            d_path = pathlib.Path(cwd).resolve() / d_path        
        dbpath = d_path.joinpath(dbname)
        if dbpath.exists() and dbpath.is_file():
            ans = input(f'\n{dbpath}\nalready exists, overwrite (y/n)?: ')
            if ans.lower() == 'y':
                return dbpath
            else:
                return None
        else:
            return dbpath


    def __create_table(self, d_path: pathlib.Path, file_type: str,
                       headers: [str]) -> bool:

        result = True
        
        drop_table_template = "drop table if exists {}"
        create_table_template = "create table if not exists '{}' ( {} );"
        column_template = "{} text"

        dbname = self.__DBNAME[file_type]
        dbpath = AOD_2db.__get_dbpath(d_path, dbname)
        if dbpath is None:
            msg = f'The file {dbname} has not been created'
            return False
        table_name = self.__DBTABLE[file_type]

        columns = [column_template.format(h1) for h1 in headers]
        columns = ', '.join(columns)

        stm0 = drop_table_template.format(table_name)
        stm = create_table_template.format(table_name, columns)
        
        conn = sqlite3.connect(dbpath)
        cur = conn.cursor()
        try:
            cur.execute(stm0)
            cur.execute(stm)
        except Exception as err:
            result = False
            msg = f'Error creating table {dbpath.name}\n{err}'
            logging.append(msg)
        finally:
            conn.close()
            self.dbpath = dbpath
            self.table_name = table_name
            return result


    def __insert_data(self, f_paths: [pathlib.Path], verbose: bool = True):
        
        stm_template = "insert into {} ({}) values ({})"

        conn = sqlite3.connect(self.dbpath)
        cur = conn.cursor()
        
        ier = 0
        for i, fp1 in enumerate(f_paths):
            if verbose:
                print(i, fp1.name)
            rows = []
            headers = AOD_2db.__get_unique_ordered_headers([fp1])
            columns = ', '.join(headers)
            with open(fp1, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for ir, row in enumerate(csv_reader):
                    if ir == 0:
                        qs = ['?' for e1 in row]
                        qs = ', '.join(qs)
                        stm = stm_template.format(self.table_name, columns, qs)
                        ncols = len(row)
                    else:
                        r1 = [str(e1) for e1 in row]
                        if ncols != len(row):
                            msg = f'file {fp1.name},\n line {ir+1:d} has' +\
                                f' {len(row)} columns but are expected '+\
                                    f'{ncols:d}' 
                            logging.append(msg)
                            ier += 1 
                            if ier == self.__MAX_ERRORS_2STOP_INSERTING:
                                msg = 'Máx number of error reading csv'
                                logging.append(msg)
                                raise ValueError(msg)
                        else:
                            rows.append(tuple(r1))

            try:
                cur.executemany(stm, rows)
                conn.commit()
            except Exception as e:
                conn.close()
                msg = f'File {fp1.name}\nError {e}'
                logging.append(msg)
                return False
        return True


