from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector as snow
import pandas as pd

# Create a connection to Snowflake
connection = snow.connect(
    account="",
    user="",
    password=""
)

# Define a cursor
cursor = connection.cursor()

# To upload the Local CSV file(csv_data_file) to Snowflake table(table_name)
def uploadCSVData(table_name, csv_data_file):
    sql = "TRUNCATE TABLE IF EXISTS "+table_name
    cursor.execute(sql)
    data = pd.read_csv("testData/"+csv_data_file, sep = ",")
    write_pandas(connection, data, table_name)

# To create new table(new_table_name) in Snowflake and upload the DataFrame(df)
def uploadSummaryTable(new_table_name, df):
    df_schema = pd.io.sql.get_schema(df, new_table_name)
    df_schema = str(df_schema).replace('TEXT', 'VARCHAR(256)').replace('CREATE TABLE', 'CREATE OR REPLACE TABLE')
    cursor.execute(df_schema)
    write_pandas(connection, df, new_table_name)

# To select the snowflake database schema(schema) to work tables under that schema
def selectSchema(schema):
    sql = "USE "+schema
    cursor.execute(sql)

# To create DataFrame from the Snowflake table(table)
def loadToPandaDF(table):
    sql = "SELECT * FROM "+table
    cursor.execute(sql)
    return cursor.fetch_pandas_all()

# To execute the sql query(sql) and Write the result into local file with write mode(append_mode)
def executeSaveToText(sql, append_mode, section):
    start = '================================ '+section+' ================================'
    data = {start:[]}
    df = pd.DataFrame(data)
    df.to_csv('analysis/statistics.txt', mode=append_mode, sep='\t', index=False)
    append_mode = 'a'
    cursor.execute(sql)
    df = cursor.fetch_pandas_all()
    df.to_csv('analysis/statistics.txt', mode=append_mode, sep='\t', index=False)

try:
    # Select Snowflake schema to work with
    selectSchema("TEST_DATABASE.TEST_SCHEMA")

    # Upload the local csv file to Snowflake database
    uploadCSVData("TOURISTS_CSV_LOAD", "Tourist_Data_CSV.csv")
    uploadCSVData("BOOKINGS_CSV_LOAD", "Bookings_Data_CSV.csv")
    uploadCSVData("ACCOMMODATION_CSV_LOAD", "Accommodation_Data_CSV.csv")

    # Cretae dataframe from the Snowflake tables
    bookingdf = loadToPandaDF("BOOKINGS_CSV_LOAD")
    touristsdf = loadToPandaDF("TOURISTS_CSV_LOAD")
    accommodationdf = loadToPandaDF("ACCOMMODATION_CSV_LOAD")
    # Merge two tables with inner join
    df = pd.merge(touristsdf, bookingdf, on="TOURIST_ID", how="inner")

    # Initialize filter objects
    options = ['American', 'Indian']
    costlimit = 900

    # Print the Filter, Aggregation results
    print('================================================== Filtering ===================================================')
    print('==Extract bookings for tourists from specific countries '+','.join(options)+' or with booking costs over a '+str(costlimit)+'==')
    print(df[(df['COUNTRY_OF_ORIGIN'].isin(options)) | (df['COST']<=costlimit)])
    print('================================================== Aggregation =================================================')
    print('------------------------------- Calculate the total cost of bookings per tourist -------------------------------')
    print(df.groupby('FIRST_NAME').agg(TOTAL_BOOKING_COST=pd.NamedAgg(column="COST", aggfunc="sum")))
    print('-------------------------------- Calculate the average rating of accommodations --------------------------------')
    print(accommodationdf.groupby('HOTEL_NAME').agg(AVERAGE_RATINGS=pd.NamedAgg(column="RATINGS", aggfunc="mean")))
    
    # Upload dataframe table to Snowflake database
    uploadSummaryTable('TOURIST_SUMMARY', df)

    # Execute the sql quries and save the results in local text file
    sql = "SELECT AVG(COST) from TOURIST_SUMMARY"
    executeSaveToText(sql, 'w', "Average booking cost")
    sql = "select destination from tourist_summary group by destination order by count(*) desc"
    executeSaveToText(sql, 'a', "Most popular destinations")
    df = sql = "SELECT AVG(DURATION) from TOURIST_SUMMARY"
    executeSaveToText(sql, 'a', "Average duration of stays")
    
finally:
    cursor.close()
connection.close()