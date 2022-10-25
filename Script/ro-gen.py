from sqlite3 import connect
import pandas.io.sql
import pyodbc
import pandas as pd
import os
from datetime import datetime

#bm.prenlyn.net server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=bm.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "bm.prenlyn" + ".parquet", index = False)

# cm.prenlyn.net server
from sqlite3 import connect
import pandas.io.sql
import pyodbc
import pandas as pd
import os
from datetime import datetime

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=cm.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "cm.prenlyn" + ".parquet", index = False)


#du.prenlyn.net server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=du.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "du.prenlyn" + ".parquet", index = False)


#ea.prenlyn.net server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=ea.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "ea.prenlyn" + ".parquet", index = False)

#ed.prenlyn.net server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=ed.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "ed.prenlyn" + ".parquet", index = False)

#en.prenlyn.net server
conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=en.prenlyn.net;'
                      'uid=RowReadOnly3P;'
                      'pwd=read0nly!rowriter;'
                      'Database=ROWriter;')

query= """SELECT DATEPART(week, CAST(pay_date as date)) as week_number,
    DATEPART(year, CAST(pay_date as date)) as year_end,
    DATEPART(quarter, CAST(pay_date as date)) as quarter_end,
    LICNO,
    CAST(pay_date as date) as pay_date,
    CAST(SUM(total - tax - st) AS MONEY) as total_sales,
    CAST(
        SUM(total - tax - st - p_cost) AS MONEY
    ) as total_gp,
    CAST(SUM(p_cost) AS MONEY) as total_cogs,
    CAST(SUM(supplies) AS MONEY) as shop_supplies,
    ro_no as ro_no,
    [status],
    CASE
        LICNO
        WHEN 70533 THEN 'Tatoian'
        WHEN 78690 THEN 'Tatoian'
        WHEN 79604 THEN 'Jeff'
        WHEN 370108 THEN 'Candace'
        WHEN 79591 THEN 'Jeff'
        WHEN 77717 THEN 'Tatoian'
        WHEN 79639 THEN 'Nesbitt'
        WHEN 76477 THEN 'Nesbitt'
        WHEN 370257 THEN 'Nesbitt'
        WHEN 79605 THEN 'Jeff'
        WHEN 79926 THEN 'Mona'
        WHEN 76476 THEN 'Tatoian'
        WHEN 370084 THEN 'Candace'
        WHEN 79625 THEN 'Jeff'
        WHEN 78028 THEN 'Nesbitt'
        WHEN 79625 THEN 'Mona'
        WHEN 370224 THEN 'Tatoian'
        WHEN 77231 THEN 'Nesbitt'
        WHEN 370324 THEN 'Candace'
        WHEN 79928 THEN 'Mona'
        WHEN 79853 THEN 'Mona'
        WHEN 370372 THEN 'Nesbitt'
        WHEN 72629 THEN 'Candace'
        WHEN 63134 THEN 'Tatoian'
        WHEN 79605 THEN 'Jeff'
        WHEN 79624 THEN 'Jeff'
        WHEN 370002 THEN 'Nesbitt'
        WHEN 79598 THEN 'Jeff'
        WHEN 72631 THEN 'Candace'
    END AS dm,
    CASE
        LICNO
        WHEN 70533 THEN 'BM'
        WHEN 78690 THEN 'CM'
        WHEN 79604 THEN 'DU'
        WHEN 370108 THEN 'EA'
        WHEN 79591 THEN 'ED'
        WHEN 77717 THEN 'EN'
        WHEN 79639 THEN 'FF'
        WHEN 76477 THEN 'FV'
        WHEN 370257 THEN 'GT'
        WHEN 79605 THEN 'IR'
        WHEN 79926 THEN 'KV'
        WHEN 76476 THEN 'LH'
        WHEN 370084 THEN 'LS'
        WHEN 79625 THEN 'LV'
        WHEN 78028 THEN 'MK'
        WHEN 79625 THEN 'NP'
        WHEN 370224 THEN 'OG'
        WHEN 77231 THEN 'PP'
        WHEN 370324 THEN 'QT'
        WHEN 79928 THEN 'RD'
        WHEN 79853 THEN 'RW'
        WHEN 370372 THEN 'RX'
        WHEN 72629 THEN 'SB'
        WHEN 63134 THEN 'SF'
        WHEN 79605 THEN 'SM'
        WHEN 79624 THEN 'SV'
        WHEN 370002 THEN 'UD'
        WHEN 79598 THEN 'WF'
        WHEN 72631 THEN 'WG'
    END AS shop,
    CASE
        LICNO
        WHEN 70533 THEN 'Bryn Mawr'
        WHEN 78690 THEN 'Colmar'
        WHEN 79604 THEN 'Dumont'
        WHEN 370108 THEN 'Easton'
        WHEN 79591 THEN 'Edison'
        WHEN 77717 THEN 'East Norriton'
        WHEN 79639 THEN 'Frankford'
        WHEN 76477 THEN 'Feasterville'
        WHEN 370257 THEN 'Germantown'
        WHEN 79605 THEN 'Irvington'
        WHEN 79926 THEN 'Kenvil'
        WHEN 76476 THEN 'Langhorne'
        WHEN 370084 THEN 'Lehigh Street'
        WHEN 79625 THEN 'Lawrenceville'
        WHEN 78028 THEN 'Market St'
        WHEN 79625 THEN 'North Plainfield'
        WHEN 370224 THEN 'Ogontz'
        WHEN 77231 THEN 'Prospect Park'
        WHEN 370324 THEN 'Quakertown'
        WHEN 79928 THEN 'Randolph'
        WHEN 79853 THEN 'Rockaway'
        WHEN 370372 THEN 'Roxborough'
        WHEN 72629 THEN 'Stroudsburg'
        WHEN 63134 THEN 'Springfield'
        WHEN 79605 THEN 'Summit'
        WHEN 79624 THEN 'Somerville'
        WHEN 370002 THEN 'Upper Darby'
        WHEN 79598 THEN 'Westfield'
        WHEN 72631 THEN 'Wind Gap'
    END AS shop_name,
    CASE
        LICNO
        WHEN 70533 THEN 'PH'
        WHEN 78690 THEN 'PH'
        WHEN 79604 THEN 'NJ'
        WHEN 370108 THEN 'AT'
        WHEN 79591 THEN 'NJ'
        WHEN 77717 THEN 'PH'
        WHEN 79639 THEN 'PH'
        WHEN 76477 THEN 'PH'
        WHEN 370257 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79926 THEN 'NJ'
        WHEN 76476 THEN 'PH'
        WHEN 370084 THEN 'AT'
        WHEN 79625 THEN 'NJ'
        WHEN 78028 THEN 'PH'
        WHEN 79625 THEN 'NJ'
        WHEN 370224 THEN 'PH'
        WHEN 77231 THEN 'PH'
        WHEN 370324 THEN 'AT'
        WHEN 79928 THEN 'NJ'
        WHEN 79853 THEN 'NJ'
        WHEN 370372 THEN 'PH'
        WHEN 72629 THEN 'AT'
        WHEN 63134 THEN 'PH'
        WHEN 79605 THEN 'NJ'
        WHEN 79624 THEN 'NJ'
        WHEN 370002 THEN 'PH'
        WHEN 79598 THEN 'NJ'
        WHEN 72631 THEN 'AT'
    END AS market,
    CASE
        LICNO
        WHEN 70533 THEN 'P1'
        WHEN 78690 THEN 'P2'
        WHEN 79604 THEN 'P3'
        WHEN 370108 THEN 'P4'
        WHEN 79591 THEN 'P3'
        WHEN 77717 THEN 'P2'
        WHEN 79639 THEN 'P2'
        WHEN 76477 THEN 'P2'
        WHEN 370257 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79926 THEN 'P3'
        WHEN 76476 THEN 'P2'
        WHEN 370084 THEN 'P4'
        WHEN 79625 THEN 'P3'
        WHEN 78028 THEN 'P2'
        WHEN 79625 THEN 'P3'
        WHEN 370224 THEN 'P1'
        WHEN 77231 THEN 'P2'
        WHEN 370324 THEN 'P4'
        WHEN 79928 THEN 'P3'
        WHEN 79853 THEN 'P3'
        WHEN 370372 THEN 'P1'
        WHEN 72629 THEN 'P4'
        WHEN 63134 THEN 'P1'
        WHEN 79605 THEN 'P3'
        WHEN 79624 THEN 'P3'
        WHEN 370002 THEN 'P1'
        WHEN 79598 THEN 'P3'
        WHEN 72631 THEN 'P4'
    END AS company
FROM dbo.ro
WHERE CAST(pay_date as date) >= '2018-01-01'
GROUP BY ro_no,
    LICNO,
    CAST(pay_date as date),
    [status]
"""

df = pd.read_sql(query, conn)
# Export the data on the test folder
df.to_parquet(os.environ["userprofile"] + "\\Documents\\Parquet-Files\\" + "en.prenlyn" + ".parquet", index = False)

#ff.prenlyn.net server
