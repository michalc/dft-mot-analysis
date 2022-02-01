import sqlite3

target_db = 'dft-mot-results-and-test-items-2005-to-2020.sqlite'

with sqlite3.connect(target_db) as con:
    cur = con.cursor()
    cur.execute("""
        SELECT
            make,
            count(*) FILTER (WHERE test_result = 'P') as pass,
            count(*) FILTER (WHERE test_result = 'F') as fail,
            CAST(count(*) FILTER (WHERE test_result = 'F') AS REAL) / (
                CAST(count(*) FILTER (WHERE test_result = 'F') AS REAL) + CAST(count(*) FILTER (WHERE test_result = 'P') AS REAL)
            ) AS pass_rate
        FROM
            tests
        WHERE
            test_result in ('P','F') AND test_class_id = 4
        GROUP BY
            make
        ORDER BY
            pass
    """)
    for row in cur:
        print(row)
