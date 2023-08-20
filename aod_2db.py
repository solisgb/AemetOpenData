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
from typing import Union

import littleLogging as logging


class AOD_2db():
    """
    Insert the meteorological data from Aemet OpenData that has been downloaded
        as csv files into a sqlite3 database with a unique table. 
    For each type of file, a database with a single table is created. 
        Therefore, each table contains a meteorological time series. 
    The name of the database is determined by the type of data to be inserted
        and for each type the name is always the same.
    Table structure:
        1) Table does not have a primary key.
        2) Columns names. They Are the the headers of the downloaded csv files
        3) Columns types. Is always TEXT (str). 
    Table contents:
        1) The inserted data is the same as the downloaded data, no formatting
        transformation is performed.
        2) If multiple download sessions of the same data type are performed
        with overlapping date ranges and ther are saved in the same directory,
        the data files contain repeated data.
        This repeated data will be inserted as-is into the table and therefore
        the table will contain repeated data. 
    """

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

    
    def __init__(self, d_path: str, file_type: str):
        """
        Parameters
        ----------
        d_path (str). Directory path where files has been saved and database 
            will be created
        file_type (str). Type of files to insert in the database, codified as
            the keys of the dictionary __FILE_PATTERNS

        Returns
        -------
        self
        """
        
        dir_path = AOD_2db.exists_dir_path(d_path)
        dir_path = AOD_2db.__get_absolute_path(dir_path)
        
        par = {'file_type': file_type}
        AOD_2db.__valid_value(par, AOD_2db.__FILE_PATTERNS.keys())
        
        self.dir_path: pathlib.Path = dir_path
        self.file_type: str = file_type
        
        self.__dbpath: pathlib.Path = None
        self.table_name: str = None


    @property
    def dbpath(self):
        return self.__dbpath


    @dbpath.setter
    def dbpath(self, dbpath: str):
        dbpath = AOD_2db.__get_absolute_path(dbpath)
        if not dbpath.exists() or not dbpath.is_file():
            msg = f"No {dbpath} exists"
            logging.append(msg)
            raise ValueError(msg)
        self.__dbpath = dbpath


    def get_dbpath(self) -> pathlib.Path:
        dbname = self.__DBNAME[self.file_type]
        dbpath = self.dir_path.joinpah(dbname)
        return dbpath 

            
    def to_db(self) :
        """
        Inserts the data in a database of type sqlite3

        Returns
        -------
        True if the task ends OK
        """
        
        f_paths = self.__get_file_paths()
        if not f_paths:
            logging.append('No files downloaded from' +\
                           f' Aemet OpenData in {self.dbpath}')
            return False
        
        headers = AOD_2db.__get_unique_ordered_headers(f_paths)
        
        if not self.__create_table(headers):
            return False
        
        if not self.__insert_data(f_paths):
            return False
        
        return True


    def to_csv(self, od_path: str, of_name: str) -> bool:
        """
        Export the unique table db to a csv file
        If you export the table in the same session you have created it, this
            is the table to export. 
        If you want to dump a previous created database, you must first
            set the value of self.dbpath

        Parameters
        ----------
        od_path (str). Directory path for output
        of_name (str). File name for output

        Returns
        -------
        bool. True if the task ends ok

        """
        
        select_template = 'select * from {}'

        if self.dbpath is None:
            msg = 'self.dbpath is not set'
            logging.append(msg)
            return False
        
        table_name = AOD_2db.__get_table_name(self.dbpath)
        select = select_template.format(table_name) 
        column_names = self.__get_columns_names(table_name)
        if not column_names:
            return False

        d_path = AOD_2db.__get_absolute_path(od_path)
        f_path = d_path.joinpath(of_name) 

        try:
            conn = sqlite3.connect(self.dbpath)
            cur = conn.cursor()
            cur.execute(select)
            data = cur.fetchall()
            conn.close()
        except sqlite3.Error as err:
            msg = f'Sqlite error {err}' 
            logging.append(msg)
            try:
                conn.close()
            except:
                pass        
            return False
        
        with open(f_path, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(column_names)
            csv_writer.writerows(data)
        msg = f'Data dumped to\n{f_path}'
        logging.append(msg)
        
        return True


    def __get_columns_names(self, table_name: str) -> [str]:
        
        select_template = "select * from {}"
        columns = []
        
        try:
            select = select_template.format(table_name)
            conn = sqlite3.connect(self.dbpath)
            cur = conn.cursor()
            data = cur.execute(select)
            columns = [col1 for col1 in data.description]
        except sqlite3.Error as err:
            msg = f'Sqlite error {err}' 
            logging.append(msg)
        except Exception as err:
            msg = f'Error {err}' 
            logging.append(msg)
        finally:
            try:
                conn.close()
            except:
                pass
            return columns       


    @staticmethod
    def __get_table_name(dbpath: pathlib.Path):
        
        SELECT = "select name from sqlite_master where type='table';"
        
        try:
            conn = sqlite3.connect(dbpath)
            cur = conn.cursor()
            cur.execute(SELECT)
            table_name = cur.fetchone()
            return table_name
        except sqlite3.Error as err:
            msg = f'Sqlite error {err}' 
            logging.append(msg)
            try:
                conn.close()
            except:
                pass
            return None 


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


    def __get_file_paths(self) -> [] :
        f_paths = self.dir_path.glob('*_data.csv')
        filtered_names = \
            [fp1 for fp1 in f_paths if \
             re.match(AOD_2db.__FILE_PATTERNS[self.file_type], fp1.name)]
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
    def __get_absolute_path(path: Union[str, pathlib.Path]) -> pathlib.Path:
        if not isinstance(path, pathlib.Path):
            path = str(path)
            path = pathlib.Path(path)
        if not pathlib.Path(path).is_absolute():
            cwd = os.getcwd()
            apath = pathlib.Path(cwd).resolve() / path
            return apath
        else:
            return path                


    def __get_file_path(self, file_name: str) -> pathlib.Path:

        file_path = self.dir_path.joinpath(file_name)
        if file_path.exists() and file_path.is_file():
            ans = input(f'\n{file_path}\nalready exists, overwrite (y/n)?: ')
            if ans.lower() == 'y':
                return file_path
            else:
                return None
        else:
            return file_path


    def __create_table(self, headers: [str]) -> bool:

        result = True
        
        drop_table_template = "drop table if exists {}"
        create_table_template = "create table if not exists '{}' ( {} );"
        column_template = "{} text"

        dbname = self.__DBNAME[self.file_type]
        dbpath = self.__get_file_path(dbname)
        if dbpath is None:
            msg = f'The file {dbname} has not been created'
            return False
        table_name = AOD_2db.__DBTABLE[self.file_type]

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
        msg = '\nPrevious downloaded files has been imported to' +\
            f' the sqlite db\n{self.dbpath}'
        logging.append(msg)
        return True


