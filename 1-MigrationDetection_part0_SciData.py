#Created on Mon May 20 14:34:52 2024

#@author: Paul Blanchard

##__ Description

# MigrationDetection_part0 imports a raw mobile phone dataset and calculates/exports daily locations for each user.
# First, hourly locations are computed as the modal location observed during the corresponding one-hour time window. 
# Then, daily locations are defined as the modal hourly location at night-time (6pm-8am) if the user is observed during that time interval, and the modal hourly location during daytime otherwise.


###
#0# Preliminary
###


##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationDetection_part0')\
    .config("spark.driver.memory", "4g")\
        .config("spark.executor.memory", "4g")\
            .config("spark.driver.maxResultSize", "10g")\
                .getOrCreate()


##__ Load packages
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql import Window


##__ Define paths
# Path to raw mobile phone dataset. Specify if the path points to a single csv file (dataset_format="csv") or a parquet folder (dataset_format="parquet"). The user can adjust the script for other formats.
MPdataset_path=""
dataset_format=""

# Path to a two-column csv file providing a correspondence between an antenna identifier and a voronoi cell identifier.
antenna_path=".csv"

# Path pointing to the folder where the final daily locations dataset is exported. (Suggested folder name "MigrationDetection_output", for consistency with other scripts)
out_dir=".../MigrationDetection_output/"


##__ Option to apply a filter and keep a subset of users, for instance to impose constraints on observational characteristics (e.g. length and frequency of observation).
# !!Note: the filter must be constructed by the user prior to executing this script, in the form of a csv (or csv.gz) file with one column giving the device_id values to select.

# Filtering dummy: if 1 then a filter is applied.
filter_dummy = 1

# Filter name, used in the name of exported outputs.
filter_name=""

# Path pointing to the filter file
filter_path=".csv"

###
#1# Import Mobile Phone dataset and filter to selected users
###


##__ Import Mobile Phone dataset with three columns of interest:
    # "device_id"=device/SIM card identifier (character)
    # "call_date"=date of the call giving the date, month, year and hour of the call (timestamp)
    # "antenna_id"=the identifier of the antenna that processed the call (character)
    # !! Names should be adjusted to match those used in the dataset of interest.
    # !! For csv file, adjust the separator argument "sep" if necessary. Set to ";" by default.

if dataset_format=="csv":
    MPdataset=spark.read.csv(MPdataset_path,header="true",sep=";")\
        .select("device_id","call_date","antenna_id")
elif dataset_format=="parquet":
    MPdataset=spark.read.parquet(MPdataset_path)\
        .select("device_id","call_date","antenna_id")


##__ Apply filter
if filter_dummy == 1:
    ## Import filter dataset containing the device_id values defining the desired subset
    userFilter=spark.read.csv(filter_path,header=True,sep=",")\
        .select("device_id_filter")
    
    
    ## Apply filter using join function with a join expression
    MPdataset = MPdataset\
        .join(userFilter, MPdataset["device_id"] == userFilter["device_id_filter"])\
            .drop("device_id_filter")
    
    del userFilter


##__ Make some adjustments if required and create useful variables
# Add missing 0 in hour of date column for hours<10. Uncomment if necessary only.
# MPdataset=MPdataset.withColumn("call_date",fn.regexp_replace("call_date","  "," 0"))

# Make sure call_date is a timestamp with the desired format (yyyy-MM-dd HH:mm:ss)
MPdataset=MPdataset\
    .withColumn("call_date",fn.to_timestamp(col("call_date"),format="yyyy-MM-dd HH:mm:ss"))

# Create a date column containing only the year, month and day of the call (yyyy-MM-dd format).
MPdataset=MPdataset\
    .withColumn("date",fn.to_date(col("call_date").cast("timestamp"),format="yyyy-MM-dd"))

# Decompose date into year, month and day columns.
MPdataset=MPdataset\
    .withColumn("year",fn.year("date"))\
        .withColumn("month",fn.month("date"))\
            .withColumn("day",fn.dayofmonth("date"))

# Create an hour column that will be used to calculate hourly locations.
MPdataset=MPdataset\
    .withColumn("hour",fn.hour("call_date"))


###
#2# Assign observations to daytime/nighttime and adjust date column so that night observations between 12am and 8am are effectively assigned to the previous day
###


##__ Create a night dummy for observations between 6pm and 8am.
MPdataset=MPdataset\
    .withColumn("night_dummy",fn.when((fn.hour("call_date")<8) | (fn.hour("call_date")>=18),1).otherwise(0))

 
##__ Adjust date column so that observations between 12am and 8am are assigned to the previous day. 
MPdataset=MPdataset\
    .withColumn("date",fn.when((fn.hour("call_date")<8),fn.date_add(col("date"),-1)).otherwise(col("date")))


###
#3# Join Voronoi cell identifier
###


##__ Import antenna dictionary (correspondence between antenna identifier and voronoi cell identifier)
# !! Adjust the separator argument "sep" if necessary. Set to ";" by default.
# Import
antenna_dico=spark.read.csv(antenna_path,header=True,sep=";")

# Select the two columns of interest, "antenna_id" and "voronoi_id", and make sure they are in character and integer format respectively.
antenna_dico=antenna_dico\
    .select(col("antenna_id").cast("string").alias("antenna_id"),col("voronoi_id").cast("integer").alias("voronoi_id"))


##__ Join voronoi identifier to mobile phone dataset, using "antenna_id" as key.
MPdataset=MPdataset\
    .join(antenna_dico,on="antenna_id")


###
#4# Hierarchical frequency-based method to determine hourly and daily locations
###


##__ Calculate hourly location dataframe
# Window partitioning the dataset by user(or device, or SIM card...), year, month, day and hour of the day.
w_hour=Window.partitionBy("device_id","year","month","day","hour")

# Group by device-location-hour and calculate the number of calls for each of these units
hourly_loc=MPdataset\
    .select("device_id","voronoi_id","year","month","day","hour","night_dummy")\
        .groupBy("device_id","year","month","day","hour","night_dummy","voronoi_id")\
            .agg(fn.count(fn.lit(1)).alias("N_obs"))

# Create a column giving the number of calls associated with the location with the maximum number of calls within each device-hour unit
hourly_loc=hourly_loc\
    .withColumn("N_obs_max",fn.max("N_obs").over(w_hour))

# As a way to select the locations with the maximum number of calls within each device-hour, keep observations where the number of calls is equal to the computed maximum N_obs_max.
hourly_loc=hourly_loc\
    .where((col("N_obs")==col("N_obs_max")))

# Cases of equality may occur: by default, keep the location with the maximum identifier
hourly_loc=hourly_loc\
    .withColumn("voronoi_id_max",fn.max("voronoi_id").over(w_hour))\
        .dropDuplicates(["msisdn","year","month","day","hour"])

# Drop variables and rename the location column corresponding to the hourly location as "hourly_loc"
hourly_loc=hourly_loc\
    .drop("N_obs","N_obs_max","voronoi_id")\
        .withColumnRenamed("voronoi_id_max","hourly_loc")
        

##__ Repeat the a similar procedure to obtain the daily location dataframe
w_day=Window.partitionBy("device_id","year","month","day","night_dummy")

daily_loc=hourly_loc\
        .select("device_id","hourly_loc","year","month","day","night_dummy")\
            .groupBy("device_id","year","month","day","night_dummy","hourly_loc")\
                .agg(fn.count(fn.lit(1)).alias("N_hoursAtLoc"))\
                    .withColumn("N_hoursAtLoc_max",fn.max("N_hoursAtLoc").over(w_day))\
                        .where((col("N_hoursAtLoc")==col("N_hoursAtLoc_max")))\
                            .withColumn("hourly_loc_max",fn.max("hourly_loc").over(w_day))\
                                .dropDuplicates(["msisdn","year","month","day","night_dummy"])\
                                    .drop("N_hoursAtLoc","N_hoursAtLoc_max","hourly_loc")\
                                        .withColumnRenamed("hourly_loc_max","daily_loc")

w_day2=Window.partitionBy("msisdn","year","month","day")

daily_loc=daily_loc\
    .withColumn("obsAtNight",fn.sum(col("night_dummy")).over(w_day2))\
        .filter((col("obsAtNight")==0) | ((col("obsAtNight")==1) & (col("night_dummy")==1)))\
            .drop("obsAtNight","night_dummy")

##__ Export daily_loc in parquet format in specified folder
daily_loc.write.mode("overwrite").parquet(out_dir+"dailyLoc_data"+("_"+filter_name if filter_dummy==1 else ""))
