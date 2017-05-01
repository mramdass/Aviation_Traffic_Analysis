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

NAME = 'Air Travel Consumer Reports'
DATA = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/2002.csv'
#DATA = 'C:/Users/mramd/Documents/CS-GY 9223 - Big Data Programming/Project/Air_Travel_Consumer_Reports.csv'
HEAD = ['Year','Month','DayofMonth','DayOfWeek','DepTime','CRSDepTime','ArrTime',\
        'CRSArrTime','UniqueCarrier','FlightNum','TailNum','ActualElapsedTime',\
        'CRSElapsedTime','AirTime','ArrDelay','DepDelay','Origin','Dest','Distance',\
        'TaxiIn','TaxiOut','Cancelled','CancellationCode','Diverted','CarrierDelay',\
        'WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay']  # Note: header=True

#sc = SparkContext('local', 'test')  # Spark 1 stuff
spark = SparkSession.builder.master('local').appName(NAME).config('spark.executor.memory', '5g').getOrCreate()  # Spark 2 stuff
df = spark.read.csv(DATA, header=True)

#df.select('Year','Month').show()  # Ideal
df.registerTempTable('ATCR')  # Not necessary unless writing SQL in one line like in the following line.

#test = spark.sql('select UniqueCarrier, sum(Distance) as SumDistance, sum(AirTime) as SumAirTime from ATCR group by UniqueCarrier')
#test.coalesce(1).write.option('header', 'true').csv('C:/Users/mramd/PycharmProjects/CS-GY 9223 - Programming in Big Data Analytics - Project/test')
#test.write.csv('C:/Users/mramd/PycharmProjects/CS-GY 9223 - Programming in Big Data Analytics - Project/test')
#test.show()

# Query to find the count of flight between source and destination for each month, each day, and day of week.
# This data can be used to manage the traffic at airports so that it can be easily scalable.
query_1 = spark.sql('select count(*) as FlightCount,' + HEAD[1] + ',' + HEAD[2] + ',' + HEAD[3] + ',' + HEAD[16] + ',' + HEAD[17]\
                    + ' from ATCR group by ' + HEAD[1] + ',' + HEAD[2] + ',' + HEAD[3] + ',' + HEAD[16] + ',' + HEAD[17]\
                    + ' order by ' + HEAD[1] + ',' + HEAD[2] + ',' + HEAD[3])
print 'Showing query_1'
query_1.coalesce(1).write.option('header', 'true').csv('C:/Users/mramd/PycharmProjects/CS-GY 9223 - Programming in Big Data Analytics - Project/query_1')

# Query to find delay in flight numbers. The idea is that this data can be used to track flight
# record of airlines which are not providing good service and actions can be taken accordingly.
query_2 = spark.sql('select * from ATCR where ' + HEAD[24] + ' > 0 order by ' + HEAD[24] + ' desc')
print 'Showing query_2'
query_2.coalesce(1).write.option('header', 'true').csv('C:/Users/mramd/PycharmProjects/CS-GY 9223 - Programming in Big Data Analytics - Project/query_2')

exit(0)