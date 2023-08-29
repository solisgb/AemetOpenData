# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 11:19:26 2023

@author: solis

version 0.6
"""
import csv
import pathlib
import os
import re
import sqlite3
import traceback
from typing import Union

import littleLogging as logging


class AOD_2db():
    """
    Incorporate the meteorological data from Aemet OpenData, which has been
        saved as CSV files, into an sqlite3 database. This database will
        comprise one or two tables: the first table will hold the actual data,
        while the second one will hold the metadata linked with the data. 
    The name of the database is determined by the type of data to be inserted
        and for each type the name is always the same.
    Table structure:
        1) Table does not have a primary key.
        2) Columns names. They are the headers of the downloaded csv files
        3) Columns types. Are always TEXT (str). 
    Table contents:
        1) Format
        * In monthly data requests, Aemet treats all the data as strings, and
        for those that are of type float, a period is used as the decimal
        separator.
        * In daily data requests, Aemet distinguishes between data of type 
        string and other data of type float. In this case, Aemet uses the comma
        as the decimal separator: in the database, this decimal separator is
        changed to a period.
        2) If multiple download sessions of the same data type are performed
        with overlapping date ranges and ther are saved in the same directory,
        the csv data files contain repeated data. The app inserts only unique
        rows.
    """

    # warning, if you change these constants you must review the code
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
    __DBTABLE_METADATA = \
        {'stations_day': 'metd_metadata',
         'station1_day': 'metd_metadata',
         'station1_month': 'metm_metadata',
         }        
    __MAX_ERRORS_2STOP_INSERTING = 3

    
    def __init__(self, d_path: str, file_type: str, verbose: bool=True):
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
        self.verbose: bool = verbose
        
        self.__dbpath: pathlib.Path = None


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


    def get_default_dbpath(self) -> pathlib.Path:
        dbname = self.__DBNAME[self.file_type]
        dbpath = self.dir_path.joinpath(dbname)
        return dbpath 


    def is_daily_file_type(self):
        if 'day' in self.file_type:
            return True
        elif 'month' in self.file_type:
            return False
        else:
            msg = f'File_type ({self.file_type})does not follow the '+\
                'required name pattern' 
            raise ValueError(msg)

            
    def to_db(self) :
        """
        Inserts the data in a database of type sqlite3

        Returns
        -------
        True if the task ends OK
        """

        files_of_type = {'data': True, 'metadata': True}
        insert_data = {'data': True, 'metadata': True}
        
        for key in files_of_type:
        
            f_paths = self.__get_file_paths(key)
            if not f_paths:
                msg = f'No {key} files in '+\
                    f'{self.dir_path} of type {self.file_type}'
                logging.append(msg)
                files_of_type[key] = False
                continue
    
            headers = AOD_2db.__get_headers(f_paths)
            
            if not self.__create_table(headers, key):
                insert_data[key] = False
                continue
        
            if not self.__insert_unique(f_paths, key):
                return False
        
            if key == 'data' and self.is_daily_file_type():
                if not self.update_decimal_separator('.'):
                    continue
        
        return True


    def read_metadata_files(self) -> {}:
        """
        Reads the metadata files and returns the characteristics of the columns
            as a dictionary having the header id as the key 

        Returns
        -------
        column characteristics as a dictionary

        """
        
        file_pattern = \
            AOD_2db.__FILE_PATTERNS[self.file_type].replace('_data',
                                                            '_metadata')
        f_paths = self.dir_path.glob('*_metadata.csv')
        f_paths_filtered = \
            [fp1 for fp1 in f_paths if re.match(file_pattern, fp1.name)]        
        if not f_paths_filtered:
            logging.append('No matadata files of type'
                           f' {self.file_type} in \n{self.dir_path}')
            return {}
        
        rows_set = set()
        for file in f_paths_filtered:
            with open(file, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    rows_set.add((row['id'], row['descripcion'],
                                  row['tipo_datos'], row['requerido']))
        
        # Converting the set to a dictionary
        columns = {}
        for element in rows_set:
            key = element[0]  # First sub-element as the key
            value = list(element[1:])  # Rest of the sub-elements as a list
            columns[key] = value

        return columns


    def update_decimal_separator(self, sep: str) -> bool:
        """
        As of the columns in the table of the database are str, I can replace
            the decimal separator using sql update
        The columns to be updated are selected from metadata of the downloaded
            csv files

        Parameters
        ----------
        sep (str). A character in ('.', ',') 

        Returns
        -------
        bool. True if operation ends ok

        """
        
        if sep not in ('.', ','):
            logging.append('Only "." or "," are allowd')
            return False
        if sep == '.':
            sep0 = ','
        else:
            sep0 = '.'
        
        stm_template = "update {} set {};"
        col_replace_template = "{} = replace({}, '{}', '{}')"

        dbpath = self.get_default_dbpath()
        if not dbpath.exists() or not dbpath.is_file():
            logging.append(f"No {dbpath} exists")       
            return False
        
        table_name = AOD_2db.__DBTABLE[self.file_type]
        
        columns = self.read_metadata_files()
        selected_cols = [k for k, v in columns.items() if v[1] == 'float']
        if not selected_cols:
            logging.append('Not columns of type float in metadata')
            return False
        cols_set = [col_replace_template.format(sc1, sc1, sep0, sep) \
                    for sc1 in selected_cols]
        cols_ready = ', '.join(cols_set)
        stm = stm_template.format(table_name, cols_ready)
        
        try:
            conn = sqlite3.connect(dbpath)
            cur = conn.cursor()            
            cur.execute(stm)
            conn.commit()
            conn.close()
            a = ', '.join(selected_cols)
            print(f'Updated decimal separator as "{sep}" in columns: {a}')
            return True
        except Exception as err:
            msg = f'Error updating the table {table_name}\n{err}'
            logging.append(msg)
            try:
                conn.close()
            except:
                pass
            return False
        

    def to_csv(self) -> bool:
        """
        Export the unique table db to a csv file

        Returns
        -------
        bool. True if the task ends ok

        """
        
        select_template = 'select * from {}'

        dbpath = self.get_default_dbpath()
        if not dbpath.exists():
            logging.append(f'{dbpath} does not exists')
            return False
        
        csvpath = dbpath.with_suffix('.csv')
        if csvpath.exists():
            ans = input(f'\n{csvpath} exists, overwrite (y/n): ? ')
            if ans.lower() != 'y':
                return False            
        
        table_name = AOD_2db.__DBTABLE[self.file_type]
        select = select_template.format(table_name) 
        column_names = self.__get_columns_names(dbpath, table_name)
        if not column_names:
            return False

        try:
            conn = sqlite3.connect(dbpath)
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
        
        with open(csvpath, 'w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(column_names)
            csv_writer.writerows(data)
        msg = f'Data dumped to\n{csvpath}'
        logging.append(msg)
        
        return True


    @staticmethod
    def __get_columns_names(dbpath:pathlib.Path , table_name: str) -> [str]:
        
        select_template = "select * from {}"
        columns = []
        
        try:
            select = select_template.format(table_name)
            conn = sqlite3.connect(dbpath)
            cur = conn.cursor()
            data = cur.execute(select)
            columns = [col1[0] for col1 in data.description]
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
            return table_name[0]
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


    def __get_file_paths(self, key:str = 'data') -> [] :
        """
        Returns a list with the file paths of downloaded files of type
            data or metadata

        Parameters
        ----------
        key, optional. If 'data' return data files, if 'atadata' metadata
            files. The default is 'data'.

        Returns
        -------
        list of file paths

        """
        if key not in ('data', 'metadata'):
            logging.append(f'Unexpected value for key: {key}')
            return []
        
        file_pattern = AOD_2db.__FILE_PATTERNS[self.file_type]
        if key == 'data':
            f_paths = self.dir_path.glob('*_data.csv')
        else:
            f_paths = self.dir_path.glob('*_metadata.csv')
            file_pattern = file_pattern.replace('_data', '_metadata')
            
        filtered_names = \
            [fp1 for fp1 in f_paths if \
             re.match(file_pattern, fp1.name)]
        return filtered_names


    @staticmethod
    def __get_headers(file_paths: [str], sort:bool = True) -> [str]:
        """
        Let file_paths a list with CSV file paths, the function returns the
            headers in the files without repetitions and optionally 
            ordered by name

        Parameters
        ----------
        csv_file_paths : List with CSV file paths
        sort: If True headers will sorted. 

        Returns
        -------
        headers

        """
        all_headers = set()
    
        for file_path in file_paths:
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                headers = next(reader, None)
                if headers:
                    all_headers.update(headers)
    
        if sort:
            return sorted(all_headers)
        else:
            return all_headers


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


    def __create_table(self, headers: [str], 
                       table: str='data') -> bool:
        """
        Create a table for data or metadata

        Parameters
        ----------
        headers (str). Column names
        table : str, optional. The default is 'data'.

        Returns
        -------
        bool 

        """
        
        drop_table_template = "drop table if exists {}"
        create_table_template = "create table if not exists '{}' ( {} );"
        column_template = "{} text"

        dbpath = self.get_default_dbpath()
        if table == 'data':
            table_name = AOD_2db.__DBTABLE[self.file_type]
        else:
            table_name = AOD_2db.__DBTABLE_METADATA[self.file_type]

        columns = [column_template.format(h1) for h1 in headers]
        columns = ', '.join(columns)

        stm0 = drop_table_template.format(table_name)
        stm = create_table_template.format(table_name, columns)
        
        try:
            conn = sqlite3.connect(dbpath)
            cur = conn.cursor()
            cur.execute(stm0)
            cur.execute(stm)
            conn.close()
            return True
        except Exception as err:
            msg = f'Error creating table {table_name}\n{err}'
            logging.append(msg)
            try:
                conn.close()
            except:
                pass
            return False


    def __insert_unique(self, f_paths: [pathlib.Path], key:str) -> bool:
        """
        Insert data in f_paths in a Sqlite database. Only unique rows are
            inserted

        Parameters
        ----------
        f_paths : paths of csv files previously downloaded from Aemet 
        key : 'data' or 'metadata'

        Returns
        -------
        bool. True if the process ends correctly

        """
        
        if key not in ('data', 'metadata'):
            logging.append(f'key has not a valid value: {key}')
            return False
 
        TEMP_TABLE = 'tempt0'
        copy_table_template = "create temp table if not exists " +\
            "{} as select * from {} where 0;"
        insert_template = "insert into {} ({}) values ({})" 
        insert_from_select_template = "insert into {} ({}) {}"
        select_template = "select distinct {} from {}"
        
        dbpath = self.get_default_dbpath()
        if key == 'data':
            table_name = AOD_2db.__DBTABLE[self.file_type]
        else:
            table_name = AOD_2db.__DBTABLE_METADATA[self.file_type]

        try:
            conn = sqlite3.connect(dbpath)
            cur = conn.cursor()
            tables = \
                cur.execute("select name from sqlite_master" 
                            " where type='table';").fetchall()
            if not tables:
                logging.append(f'No tables in {dbpath.name}')
                return False
            table_names = [table[0] for table in tables]
            if table_name not in table_names:
                logging.append(f'Table {table_name} does not exists')
                return False
            cur.execute(copy_table_template.format(TEMP_TABLE, table_name))
            cur.execute(f"delete from {table_name}")
            conn.commit()            

            for i, fp1 in enumerate(f_paths):
                if self.verbose:
                    print(i, fp1.name)
                headers = []
                data = []                
                with open(fp1, 'r', encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    
                    # Extract headers from the first row
                    headers = next(csv_reader)
                    column_names = ', '.join(headers)
                    qs = ['?' for c1 in headers]
                    qs = ', '.join(qs)
                    insert_stm = insert_template.format(TEMP_TABLE, 
                                                        column_names, qs)

                    for row in csv_reader:
                        data.append(row)      

                cur.executemany(insert_stm, data)
                conn.commit()

            query = f"PRAGMA table_info({table_name})"
            cur.execute(query)
            table_info = cur.fetchall()
            column_names = [c1[1] for c1 in table_info]
            column_names = ', '.join(column_names)
            
            select = select_template.format(column_names, TEMP_TABLE)
            insert = insert_from_select_template.format(table_name, column_names,
                                                        select)

            cur.execute(insert)
            conn.commit()
        except Exception as err:
            try:
                conn.close()
            except:
                pass
            traceback_entry = \
                traceback.extract_tb(err.__traceback__)[-1]
            filename, lineno, name, line = traceback_entry            
            msg = f'\n{filename}\nLine {lineno} in {name}: {line}\n{err}'
            logging.append(msg)
            return False
                
        msg = f'\n{key} has been inserted into {table_name}'
        logging.append(msg)
        return True
