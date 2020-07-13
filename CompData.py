# System Libraries
import os
import sys
Cred_Path = '/' + sys.path[0][1:27] + 'Credentials/Credentials.csv' # Create credentials path

# Database Connector
import pymysql

# Data Manipulator
import pandas as pd
import numpy as np
import random

pd.set_option('display.max_columns', None)

# Accessory Libraries
import time

###################################################################################################################

# Creates the query for the final Data set we will be using
def complete_data(Base_Columns, Base_Table, *argv):

    '''    
    Function:
    ---------
    
    Takes in the base table columns and the base table in a string format and
    returns a structured SQL query as a string for our project.
    
    
    Future Functionality:
    ---------------------
    
    Adding more tables to the query's final form.
    
    
    Description:
    ------------
    
    This will return our query for the final construction of the data table.
    
    '''
    
    if len(argv) == 0:
        Bureau_Trim_Filter = ''
    else:
        Bureau_Trim_Filter = '\n      WHERE ' + argv[0]

    Comp_Query = '''

     WITH Base AS
     (
      SELECT {0}
      FROM {1}
     ), Credit_Trimmed AS
     (
      SELECT SK_ID_PREV, SK_ID_CURR,

             SUM(SK_DPD) AS TOTAL_SK_DPD,
             SUM(SK_DPD_DEF) AS TOTAL_SK_DPD_DEF,

             SUM(CASE WHEN SK_DPD > 0 THEN 1 ELSE 0 END) AS CNT_PAST_DUE,
             SUM(CASE WHEN SK_DPD_DEF > 0 THEN 1 ELSE 0 END) AS CNT_PAST_DUE_DEF

      FROM Credit_Card_Balance
      GROUP BY 1, 2
     ), Bureau_Partitioned AS
     (
      SELECT SK_ID_CURR, CREDIT_ACTIVE, CREDIT_CURRENCY,

             DAYS_CREDIT_UPDATE,
             AMT_CREDIT_SUM,
             AMT_CREDIT_SUM_DEBT, 
             AMT_CREDIT_SUM_OVERDUE,

             DENSE_RANK() OVER(PARTITION BY SK_ID_CURR
                               ORDER BY AMT_CREDIT_SUM DESC) AS RANKING
      FROM Bureau
      WHERE AMT_CREDIT_SUM_DEBT != 0
     ), Bureau_Trim AS
     (
      SELECT SK_ID_CURR, CREDIT_ACTIVE, CREDIT_CURRENCY,

             SUM(CASE WHEN AMT_CREDIT_SUM > 0 THEN 1 ELSE 0 END) AS CNT_CREDIT_SUM,
             SUM(CASE WHEN AMT_CREDIT_SUM_DEBT > 0 THEN 1 ELSE 0 END) AS CNT_CREDIT_SUM_DEBT, 
             SUM(CASE WHEN AMT_CREDIT_SUM_OVERDUE > 0 THEN 1 ELSE 0 END) AS CNT_CREDIT_SUM_OVERDUE

      FROM Bureau {2}
      GROUP BY 1, 2, 3
     ), Bureau_Base_Credit AS
     (
      SELECT a.*, 

             b.CREDIT_ACTIVE, b.CREDIT_CURRENCY,
             b.CNT_CREDIT_SUM, b.CNT_CREDIT_SUM_DEBT, b.CNT_CREDIT_SUM_OVERDUE,

             c.DAYS_CREDIT_UPDATE, c.AMT_CREDIT_SUM,
             c.AMT_CREDIT_SUM_DEBT, c.AMT_CREDIT_SUM_OVERDUE,

             d.SK_ID_PREV, d.CNT_PAST_DUE,
             d.CNT_PAST_DUE_DEF

      FROM Base a LEFT JOIN Bureau_Trim b
      ON a.SK_ID_CURR = b.SK_ID_CURR
      LEFT JOIN (SELECT * 
                 FROM Bureau_Partitioned
                 WHERE RANKING = 1) c
      ON a.SK_ID_CURR = c.SK_ID_CURR
      LEFT JOIN Credit_Trimmed d
      ON a.SK_ID_CURR = d.SK_ID_CURR
     )

     SELECT * 
     FROM Bureau_Base_Credit
     ORDER BY SK_ID_CURR;

     '''.format(Base_Columns, Base_Table, Bureau_Trim_Filter)
    
    return Comp_Query

# Generalized Querying Module
def get_data(Query):
    
    '''
    Function:
    ---------
    
    Takes in a MySQL Query in a string format and return Pandas Data Frame.
    
    
    Future Functionality:
    ---------------------
    
    We will be able to change the DB and the host + port locatons.
    
    
    Description:
    ------------
    
    This will be an isolated instance querying function where it takes in a SQL query 
    and returns a Pandas Data Frame. The query should be in a string format.
    
    '''
    
    # Pulling in Credentials
    Credentials = eval(pd.read_csv(Cred_Path, header = None).iloc[0,0])

    # Establishing Connection
    connector = pymysql.connect(port = Credentials['port'],
                                user = Credentials['user'], passwd = Credentials['pass'],
                                db = 'Housing', charset = 'utf8mb4')
    Start = time.perf_counter()
    
    Complete = pd.read_sql_query(Query, connector)
    
    End = time.perf_counter()
    
    print('This query took {0} seconds to run.'.format(round(End - Start, 2)))
    
    connector.close()
    
    return Complete

# Used to get the tables within a Data Base
def get_tables():
    
    '''
    Function:
    ---------
    
    Takes in no inputs since we are focued on one Data Base.
    
    
    Future Functionality:
    ---------------------
    
    We will be able to change the DB and the host + port locatons.
    
    
    Description:
    ------------
    At the present moment, this function takes in no parameters but will return
    all available tables within the Data Base in a Pandas Data Frame.
    
    '''
    
    # Pulling in Credentials
    Credentials = eval(pd.read_csv(Cred_Path, header = None).iloc[0,0])

    # Establishing Connection
    connector = pymysql.connect(port = Credentials['port'],
                                user = Credentials['user'], passwd = Credentials['pass'],
                                db = 'Housing', charset = 'utf8mb4')
    
    Tables = '''
    
    SHOW TABLES;

    '''
    
    Table_Choices = pd.read_sql_query(Tables, connector)
    
    connector.close()
    
    return Table_Choices

# Extracts only the important columns that we need for table creation
def colextract(Data):
    
    '''
    Function:
    ---------
    
    Takes Data Frame Object and returns a SQL format string of truncated columns.
    
    
    Future Functionality:
    ---------------------
    
    Flexibility to choose the columns to truncate.
    
    
    Description:
    ------------
    This will take in the columns and trim them down to the important ones.
    It will return a string formated output for easy processing with SQL.
    
    '''
    
    Structure = pd.DataFrame(data = (list(Data.columns), list(Data.dtypes)),
                             index = ['colnames', 'dtypes'])
    Structure = Structure.T
    Structure['dtypes'] = np.where(Structure['dtypes'] == 'object', 'VARCHAR(200)', Structure['dtypes'])
    Structure['dtypes'] = np.where(Structure['dtypes'] == 'float64', 'FLOAT', Structure['dtypes'])
    Structure['dtypes'] = np.where(Structure['dtypes'] == 'int64', 'BIGINT', Structure['dtypes'])
    Structure['Concat'] = ['{0} {1}'.format(Structure.iloc[i,0], Structure.iloc[i,1]) for i in range(len(Structure))]

    Columns = str(tuple(Structure['Concat']))
    
    return Columns.replace("'", "")

# Extracts only the important columns that we need for querying
def get_core_columns(Col_List, *argv):
    
    '''
    Function:
    ---------
    
    Takes in columns dictionary generated by the get_table_keys_columns function and returns
    a string of trimmed columns.
    
    
    Future Functionality:
    ---------------------
    
    Flexibility to choose the columns to truncate.
    
    Description:
    ------------
    This will take in a table name of choice and the column dictionary created by the 
    get_table_key_columns function and returns a stringed set of columns for the SQL query
    to execute.
    
    '''
    if len(argv) > 0:
        Table_Columns = Col_List[argv[0]]
    else:
        Table_Columns = Col_List
    
    # Trimming Base Table by the following column names
    App_Train_Trim = [i for i in Table_Columns if '_MODE' not in i]
    App_Train_Trim = [i for i in App_Train_Trim if '_MEDI' not in i]
    App_Train_Trim = [i for i in App_Train_Trim if 'FLAG_DOCUMENT_' not in i]
    App_Train_Trim = [i for i in App_Train_Trim if 'REGION_RATING_CLIENT' != i]

    Application_Column_Tapper = str(App_Train_Trim)
    Application_Column_Tapper = Application_Column_Tapper.replace("'", '')\
                                                                     .replace('[', '')\
                                                                     .replace(']', '')
    
    return Application_Column_Tapper

# Used to explore a table in SQL
def explore(Table):
    
    '''
    Function:
    ---------
    
    Takes in Table Names in your Data Base and returns first 3 rows of data
    in a Pandas Data Frame.
    
    
    Future Functionality:
    ---------------------
    
    We will be able to change the DB and the host + port locatons.
    
    
    Description:
    ------------
    
    This function will allow us to explore a table in the MySQL database without
    the need to query the entire table which will take some time to retrive.
    The function will return a Pandas Data Frame with 3 rows of data.
    
    '''
    
    # Pulling in Credentials
    Credentials = eval(pd.read_csv(Cred_Path, header = None).iloc[0,0])

    # Establishing Connection
    connector = pymysql.connect(port = Credentials['port'],
                                user = Credentials['user'], passwd = Credentials['pass'],
                                db = 'Housing', charset = 'utf8mb4')
    
    Query = '''

    SELECT * 
    FROM {0}
    LIMIT 3;
    
    '''.format(Table)
    
    explored_table = pd.read_sql_query(Query, connector)
    
    connector.close()
    
    return explored_table

# Grabs the keys and the columns of our table of choice
def get_table_keys_columns(Tables):
    
    '''
    Function:
    ---------
    
    Takes in the table name in string format and returns the keys/columns for that table.
    
    
    Future Functionality:
    ---------------------
    
    Dependent of the explore function and more updates will be done on modularizing the code.
    
    
    Description:
    ------------
    This is take in a tablename, process the information within the table and return the keys and
    all the columns within the data table.
    
    '''
    
    # Placeholder to store the keys
    keys = {}
    columns = {}

    # Looping over all of the Data sets and grabbing the keys
    for i in list(Tables['Tables_in_housing']):
        columns[i] = list(explore(i).columns)
        keys[i] = [j for j in columns[i] if 'SK_ID_' in j]

    Key_DataFrame = pd.DataFrame(index = keys.keys(), columns = ['Key Columns'])
    Key_DataFrame['Key Columns'] = list(keys.values())
    
    return columns, keys

def get_missing(Data):
    
    '''
    Function:
    ---------
    
    Takes in a Pandas Data Frame and outputs another for missing value information.
    
    
    Future Functionality:
    ---------------------
    
    Relatively few, should be good for now but may add more metrics for consideration.
    
    
    Description:
    ------------
    
    This will return a Pandas Data Frame computing missing values and displaying
    data types.
    
    '''
    
    missing_table = pd.DataFrame(columns = ['Missing Count', 'Missing Percent', 'Data Types'],
                                 index = list(Data.columns))
    
    missing_table['Missing Count'] = list(Data.isnull().sum())
    
    missing_table['Missing Percent'] = ['{0}%'.format(round((100 * i)/len(Data), 2))
                                        for i in missing_table['Missing Count']]
    
    missing_table['Data Types'] = list(Data.dtypes)
    
    return missing_table.sort_values(by = 'Missing Count', ascending = False).T

def get_missing_partition(Data):
    
    '''
    Function:
    ---------
    
    Takes in a Pandas Data Frame of missing values generated by the get_missing 
    function and returns 3 lists of missing value columns.
    
    
    Future Functionality:
    ---------------------
    
    Adding more data types as they come to cover more cases.
    
    
    Description:
    ------------
    
    This will return 3 lists of missing value column names and give us the breakdown.
    The order in which it is returned is Floats, Objects, and then Integers.
    
    '''
    
    Missing_Floats = [Data.columns[i] 
                      for i in range(len(Data.columns)) 
                      if Data.iloc[0,i] > 0 and 'float' in str(Data.iloc[2,i])]

    Missing_Objects = [Data.columns[i] 
                       for i in range(len(Data.columns)) 
                       if Data.iloc[0,i] > 0 and 'object' in str(Data.iloc[2,i])]

    Missing_Integers = [Data.columns[i] 
                        for i in range(len(Data.columns)) 
                        if Data.iloc[0,i] > 0 and 'int' in str(Data.iloc[2,i])]
    
    print('''\nThere are {0} missing float, {1} missing object, and {2} missing integer columns.'''\
          .format(len(Missing_Floats), len(Missing_Objects), len(Missing_Integers)))

    return Missing_Floats, Missing_Objects, Missing_Integers

def fill_missing(data, column_of_interest):
    
    '''
    Function:
    ---------
    
    Takes in two parameters, the Pandas DataFrame and the Column that we are
    trying to correct in a string format.
    
    
    Future Functionality:
    ---------------------
    
    None at the present moment, but as we proceed we will think of new things
    to implement.
    
    
    Description:
    ------------
    
    This will return a list of values that will be used to fill in the missing
    terms in the Pandas DataFrame. The idea is pretty simple, sample from the
    DataFrame where the values are not missing, then take those values and shift
    it by a same factor and randomly subtract or add this term to the sampled
    point. This will generate a new point and maintain the distribution shape.
    
    '''
    
    Sample = random.choices(list(data.loc[data[column_of_interest].isnull() == False, 
                                          column_of_interest]),
                            k = data[column_of_interest].isnull().sum())
    
    Sign = random.choices([-2, -1, 1, 2], k = data[column_of_interest].isnull().sum())
    
    def osicallation(point, sign, random_osicallation_point):
        return (point + sign * random_osicallation_point)
    
    Sample_Oscilated = []
    
    for i, j in zip(Sample, Sign):
        
        osicallation_point = random.random()
        
        if osicallation(i, j, osicallation_point) > 0:
            Sample_Oscilated.append(osicallation(i, j, osicallation_point))
        else:
            Sample_Oscilated.append(0)
    
    return Sample_Oscilated

###################################################################################################################