var a1 = spark.read.csv("/user/on371/Aviation_Data/Air_Travel_Consumer_Reports.csv")
import spark.implicits._
a1.registerTempTable("ConsumerReport")
import org.apache.spark.sql.functions._


# Query to find the count of ConsumerReport between source and destination for each Year, Month, DayofMonth, DayOfWeek
# Done in driver.py

# 4 query to find total number of ConsumerReport delayed or diverted during the year
spark.sql("select _c23,_c21,count(*) from ConsumerReport where _c23=1 or _c21=1 group by _c23,_c21")
# 5 query to find total number of ConsumerReport delayed or diverted during the year on monthly basis.
spark.sql("select _c23 as diverted,_c21 as cancelled,_c1,count(*) from ConsumerReport where _c23=1 or _c21=1 group by _c23,_c21,_c1 order by _c1")
# 5 query to find total number of ConsumerReport delayed or diverted during the year for each Airlines
spark.sql("select _c9,count(*)as Cancelled from ConsumerReport where _c21=1 group by _c9 order by _c9")

# 7 query to find total number of ConsumerReport  Diverted during the year for source and destination pair
spark.sql("select _c16,_c17,count(*)as Diverted from ConsumerReport where _c23=1 group by _c16,_c17")
# 8 query to find total number of ConsumerReport  Cancelled during the year for source and destination pair
spark.sql("select _c16,_c17,count(*)as Cancelled from ConsumerReport where _c21=1 group by _c16,_c17")

# 9 below query show monthly travel data for fall,spring,summer and Winter.

spark.sql("select count(*) as Wintercount from ConsumerReport where _c1 in (12,1,2)  order by count desc ")
spark.sql("select count(*) as Springcount from ConsumerReport where _c1 in (3,4,5)  order by Springcount desc ")
spark.sql("select count(*) as Summercount from ConsumerReport where _c1 in (6,7,8)  order by Summercount desc ")
spark.sql("select count(*) as Fallcount from ConsumerReport where _c1 in (9,10,11)  order by Fallcount desc ")

# 13 below query show monthly travel data for particular destination.
spark.sql("select _c17,count(*) as count from ConsumerReport where _c1 in (12,1,2) group by _c17 order by count desc ")
spark.sql("select _c17,count(*) as count from ConsumerReport where _c1 in (3,4,5) group by _c17 order by count desc ")
spark.sql("select _c17,count(*) as count from ConsumerReport where _c1 in (6,7,8) group by _c17 order by count desc ")
spark.sql("select _c17,count(*) as count from ConsumerReport where _c1 in (9,10,11) group by _c17 order by count desc ")

# 17 below query used to find the average arrival delay for each Airlines travel
spark.sql("select avg(_c14),_c9  from ConsumerReport where _c14!=0 group by _c9 order by _c9")
# 18 below query used to find the average Departure delay for each Airlines travel
spark.sql("select avg(_c15),_c9  from ConsumerReport where _c15!=0 group by _c9 order by _c9")


# 19 count of ConsumerReport for each destination for a seaon-------
# This queries fetches the count of flight to each  destination in the all seasons.The result can be used for determining where does people prefer to travel during each season .
spark.sql("select _c17 as destination,count(*)  as count from ConsumerReport where _c1 in (12,1,2) group by _c17 order by count desc ")
spark.sql("select _c17 as destination,count(*) as count from ConsumerReport where _c1 in (3,4,5) group by _c17 order by count desc ")
spark.sql("select _c17 as destination,count(*) as count from ConsumerReport where _c1 in (9,10,11) group by _c17 order by count desc ")
spark.sql("select _c17 as destination,count(*) as count from ConsumerReport where _c1 in (6,7,8) group by _c17 order by count desc ")

# This queries fetches the count of flight to each  source in the all seasons.The result can be used for determining people travelling most from which part of the world during each season .
# 23 queries where you can use cordinates to show the travel during each season
spark.sql("select a._c5,a._c6 from Airport a where a._c2 in (select _c17 from ConsumerReport where _c1 in (12,1,2))")
spark.sql("select a._c5,a._c6 from Airport a where a._c2 in (select _c17 from ConsumerReport where _c1 in (3,4,5))")
spark.sql("select a._c5,a._c6 from Airport a where a._c2 in (select _c17 from ConsumerReport where _c1 in (6,7,8))")
spark.sql("select a._c5,a._c6 from Airport a where a._c2 in (select _c17 from ConsumerReport where _c1 in (9,10,11))")

# This queries have the same intent as of(23-27) with only difference that it takes the cordinates to show the result on a map .
# 27 count of ConsumerReport for each source for a seaon
spark.sql("select _c16 as source,count(*) as count from ConsumerReport where _c1 in (12,1,2) group by _c16 order by count desc ")
spark.sql("select _c16 as source,count(*) as count from ConsumerReport where _c1 in (3,4,5) group by _c16 order by count desc ")
spark.sql("select _c16 as source,count(*) as count from ConsumerReport where _c1 in (6,7,8) group by _c16 order by count desc ")
spark.sql("select _c16 as source,count(*) as count from ConsumerReport where _c1 in (9,10,11) group by _c16 order by count desc ")

