import sqlite3
import os
import csv
import re

raise Exception("DONE")

_RE_COMBINE_WHITESPACE = re.compile(r"\s+")
file_dir = 'data'
target_db = 'dft-mot-results-and-test-items-2005-to-2020.sqlite'

test_files = [
    ('|', 'test_result_2005.txt'),
    ('|', 'test_result_2006.txt'),
    ('|', 'test_result_2007.txt'),
    ('|', 'test_result_2008.txt'),
    ('|', 'test_result_2009.txt'),
    ('|', 'test_result_2010.txt'),
    ('|', 'test_result_2011.txt'),
    ('|', 'test_result_2012.txt'),
    ('|', 'test_result_2013.txt'),
    ('|', 'test_result_2014.txt'),
    ('|', 'test_result_2015.txt'),
    ('|', 'test_result_2016.txt'),
    (',', 'test_result_2017.csv'),
    (',', 'dft_test_result-from-2018-01-01_00-00-01-to-2018-04-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2018-04-01_00-00-01-to-2018-07-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2018-07-01_00-00-01-to-2018-10-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2018-10-01_00-00-01-to-2019-01-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2019-01-01_00-00-01-to-2019-04-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2019-04-01_00-00-01-to-2019-07-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2019-07-01_00-00-01-to-2019-10-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2019-10-01_00-00-01-to-2020-01-01_00-00-01.csv'),
    (',', 'dft_test_result-from-2020-01-01_00-00-00-to-2020-04-01_00-00-00.csv'),
    (',', 'dft_test_result-from-2020-04-01_00-00-00-to-2020-07-01_00-00-00.csv'),
    (',', 'dft_test_result-from-2020-07-01_00-00-00-to-2020-10-01_00-00-00.csv'),
    (',', 'dft_test_result-from-2020-10-01_00-00-00-to-2021-01-01_00-00-00.csv'),
]

test_files = [
    (',', 'test_result_2017.csv'),
]

test_item_files = [
    ('|', 'test_item_2005.txt'),
    ('|', 'test_item_2006.txt'),
    ('|', 'test_item_2007.txt'),
    ('|', 'test_item_2008.txt'),
    ('|', 'test_item_2009.txt'),
    ('|', 'test_item_2010.txt'),
    ('|', 'test_item_2011.txt'),
    ('|', 'test_item_2012.txt'),
    ('|', 'test_item_2013.txt'),
    ('|', 'test_item_2014.txt'),
    ('|', 'test_item_2015.txt'),
    ('|', 'test_item_2016.txt'),
    (',', 'test_item_2017.csv'),
    (',', 'dft_test_item-from-2018-01-01_00-00-01-to-2018-04-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2018-04-01_00-00-01-to-2018-07-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2018-07-01_00-00-01-to-2018-10-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2018-10-01_00-00-01-to-2019-01-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2019-01-01_00-00-01-to-2019-04-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2019-04-01_00-00-01-to-2019-07-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2019-07-01_00-00-01-to-2019-10-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2019-10-01_00-00-01-to-2020-01-01_00-00-01.csv'),
    (',', 'dft_test_item-from-2020-01-01_00-00-00-to-2020-04-01_00-00-00.csv'),
    (',', 'dft_test_item-from-2020-04-01_00-00-00-to-2020-07-01_00-00-00.csv'),
    (',', 'dft_test_item-from-2020-07-01_00-00-00-to-2020-10-01_00-00-00.csv'),
    (',', 'dft_test_item-from-2020-10-01_00-00-00-to-2021-01-01_00-00-00.csv'),
]

os.unlink(target_db)

with sqlite3.connect(target_db) as con:
    print('Creating database')
    cur = con.cursor()
    cur.execute('BEGIN')
    cur.execute('''
        CREATE TABLE tests (
            test_id INTEGER NOT NULL,
            vehicle_id INTEGER NOT NULL,
            test_date DATE NOT NULL,
            test_class_id INTEGER NOT NULL,
            test_type TEXT NOT NULL,
            test_result TEXT NOT NULL,
            test_mileage INTEGER NOT NULL,
            postcode_area TEXT NOT NULL,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            colour TEXT NOT NULL,
            fuel_type TEXT NOT NULL,
            cylinder_capacity INTEGER NOT NULL,
            first_use_date INTEGER NOT NULL
        )
    ''')
    cur.execute('''
        CREATE TABLE test_items (
            test_id INTEGER NOT NULL,
            rfr_id INTEGER NOT NULL,
            rfr_type_code TEXT NOT NULL,
            location_id INTEGER NOT NULL,
            dangerous_mark TEXT NOT NULL
        )
    ''')
    for delimeter, file_name in test_files:
        print(file_name)
        with open(f'{file_dir}/{file_name}', 'r') as f:
            reader = csv.reader(f, delimiter=delimeter)
            for row in reader:
                # There are issues with make and model - sometime model seems split into
                # too many extra columns - suspect file was generated without escaping the
                # delimeter
                if len(row) > 14:
                    make_cols = len(row) - 14
                    before_make = row[:9]
                    make = row[9:9+make_cols+1]
                    make = [_RE_COMBINE_WHITESPACE.sub(' ', delimeter.join(make)).strip()]
                    after_make = row[9+make_cols+1:]
                    row = before_make + make + after_make
                    print('Cleaned', row)
                cur.executemany("INSERT INTO tests VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row,))

    for delimeter, file_name in test_item_files:
        print(file_name)
        with open(f'{file_dir}/{file_name}', 'r') as f:
            reader = csv.reader(f, delimiter=delimeter)
            cur.executemany("INSERT INTO test_items VALUES (?, ?, ?, ?, ?)", reader)
    cur.execute('COMMIT')
    print('Done')
