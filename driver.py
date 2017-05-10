'''
    Team:   Munieshwar (Kevin) Ramdass
            Om Narayan
    Professor Juan Rodriguez
    CS-GY 9223 - Programming in Big Data
    22nd April 2017
    
    Air Traffic Analysis
    Execute: spark-submit driver.py
'''
import os, sys
spark_path = 'C:/spark-2.1.0-bin-hadoop2.7'
hadoop_path = ''  # not necessary; looks for winuntils.exe which is now in spark_path/bin

os.environ['SPARK_HOME'] = spark_path
os.environ['HADOOP_HOME'] = spark_path

sys.path.append(spark_path + "/bin")
sys.path.append(spark_path + "/python")
sys.path.append(spark_path + "/python/pyspark/")
sys.path.append(spark_path + "/python/lib")
sys.path.append(spark_path + "/python/lib/pyspark.zip")
sys.path.append(spark_path + "/python/lib/py4j-0.10.4-src.zip")

from pyspark import SparkConf, SparkContext  # Spark 1 stuff
from pyspark.sql import SparkSession  # Spark 2 stuff

OUT = 'C:/Users/mramd/PycharmProjects/CS-GY 9223 - Programming in Big Data Analytics - Project'
NAME = 'Air Travel Consumer Reports'
DATA = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/2002.csv'
AIRP = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/airports.csv'
TAIL = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/plane-data.csv'
CARR = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/carriers.csv'
#DATA = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/Air_Travel_Consumer_Reports.csv'
HEAD = ['Year','Month','DayofMonth','DayOfWeek','DepTime','CRSDepTime','ArrTime',\
        'CRSArrTime','UniqueCarrier','FlightNum','TailNum','ActualElapsedTime',\
        'CRSElapsedTime','AirTime','ArrDelay','DepDelay','Origin','Dest','Distance',\
        'TaxiIn','TaxiOut','Cancelled','CancellationCode','Diverted','CarrierDelay',\
        'WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']  # Note: header=True

#sc = SparkContext('local', 'test')  # Spark 1 stuff
spark = SparkSession.builder.master('local').appName(NAME).config('spark.executor.memory', '2g').getOrCreate()  # Spark 2 stuff
df = spark.read.csv(DATA, header=True)
df_airports = spark.read.csv(AIRP, header=True)
df_plane_data = spark.read.csv(TAIL, header=True)
df_carriers =  spark.read.csv(CARR, header=True)

#df.select('Year','Month').show()  # Ideal
df.registerTempTable('ATCR')
df_airports.registerTempTable('airports')
df_plane_data.registerTempTable('planes')  # Might not have use for this
df_carriers.registerTempTable('carriers')


for i in [0, 1, 2]:
    # Query to find the count of flight between source and destination for each Year, Month, DayofMonth, DayOfWeek
    query = spark.sql('select distinct count(*) as FlightCount,' + HEAD[i] + ',' + HEAD[16] + ',' + HEAD[17]\
                        + ' from ATCR group by ' + HEAD[i] + ',' + HEAD[16] + ',' + HEAD[17]\
                        + ' order by ' + HEAD[i])
    query.coalesce(1).write.option('header', 'true').csv(OUT + 'query_' + str(i + 1))

# Where are most flights delayed?
query_4 = spark.sql('select distinct sum(' + HEAD[24] + '),' + HEAD[16] + ',' + HEAD[17]\
                    + ' from ATCR group by ' + HEAD[16] + ',' + HEAD[17] + ' order by ' + HEAD[24] + ' desc')
query_4.coalesce(1).write.option('header', 'true').csv(OUT + '/query_4')

for i in [0, 1, 2]:
    # Query to find the total distance of flight between source and destination for each Year, Month, DayofMonth, DayOfWeek
    query = spark.sql('select distinct count(*) as FlightDistance,' + HEAD[i]\
                        + ' from ATCR group by ' + HEAD[i]\
                        + ' order by FlightDistance desc')
    query.coalesce(1).write.option('header', 'true').csv(OUT + '/query_' + str(i + 5))

# Queries 8 to 15 will be used to see where people generally travel to seasonally - Do they head south during the winter?
# Find the number of flights in the three months of Dec, Jan, and Feb for each year
query_8 = spark.sql('select count(*) as FlightCount,' + HEAD[0] + ' from ATCR where ' + HEAD[1] + '==12 or ' + HEAD[1] + '==1 or ' + HEAD[1] + '==2'\
                    + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_8.coalesce(1).write.option('header', 'true').csv(OUT + '/query_8')

# Find the number of flights in the three months of Mar, Apr, and May for each year
query_9 = spark.sql('select count(*) as FlightCount,' + HEAD[0] + ' from ATCR where ' + HEAD[1] + '==3 or ' + HEAD[1] + '==4 or ' + HEAD[1] + '==5'\
                    + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_9.coalesce(1).write.option('header', 'true').csv(OUT + '/query_9')

# Find the number of flights in the three months of Jun, Jul, and Aug for each year
query_10 = spark.sql('select count(*) as FlightCount,' + HEAD[0] + ' from ATCR where ' + HEAD[1] + '==6 or ' + HEAD[1] + '==7 or ' + HEAD[1] + '==8'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_10.coalesce(1).write.option('header', 'true').csv(OUT + '/query_10')

# Find the number of flights in the three months of Sep, Oct, and Nov for each year
query_11 = spark.sql('select count(*) as FlightCount,' + HEAD[0] + ' from ATCR where ' + HEAD[1] + '==9 or ' + HEAD[1] + '==10 or ' + HEAD[1] + '==11'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_11.coalesce(1).write.option('header', 'true').csv(OUT + '/query_11')

# Winter Travels
query_12 = spark.sql('select distinct count(*) as FlightCount,' + HEAD[16] + ',' + HEAD[17]\
                     + ' from ATCR where ' + HEAD[1] + '==12 or ' + HEAD[1] + '==1 or ' + HEAD[1] + '==2'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_12.coalesce(1).write.option('header', 'true').csv(OUT + '/query_12')

# Spring Travels
query_13 = spark.sql('select distinct count(*) as FlightCount,' + HEAD[16] + ',' + HEAD[17]\
                     + ' from ATCR where ' + HEAD[1] + '==3 or ' + HEAD[1] + '==4 or ' + HEAD[1] + '==5'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_13.coalesce(1).write.option('header', 'true').csv(OUT + '/query_13')

# Summer Travels
query_14 = spark.sql('select distinct count(*) as FlightCount,' + HEAD[16] + ',' + HEAD[17]\
                     + ' from ATCR where ' + HEAD[1] + '==6 or ' + HEAD[1] + '==7 or ' + HEAD[1] + '==8'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_14.coalesce(1).write.option('header', 'true').csv(OUT + '/query_14')

# Fall Travels
query_15 = spark.sql('select distinct count(*) as FlightCount,' + HEAD[16] + ',' + HEAD[17]\
                     + ' from ATCR where ' + HEAD[1] + '==9 or ' + HEAD[1] + '==10 or ' + HEAD[1] + '==11'\
                     + ' group by ' + HEAD[0] + ' order by FlightCount desc')
query_15.coalesce(1).write.option('header', 'true').csv(OUT + '/query_15')
# End of Seasonal Analysis
