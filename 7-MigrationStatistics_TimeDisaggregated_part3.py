# Created on Mon May  1 12:34:38 2023

# @author: Paul Blanchard

##__ Description

# This script imports user-level meso-segments and calculates migration stock by origin-destination and specified time unit.
# !!! It uses a temporary output produced in script 6, which thus needs to be executed prior to running this script.


###
#0# Preliminary
###


##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationStatistics_part3')\
    .config("spark.driver.memory", "10g")\
        .config("spark.executor.memory", "10g")\
            .config("spark.driver.maxResultSize", "5g")\
                .config("spark.sql.autoBroadcastJoinThreshold","-1")\
                    .getOrCreate()

##__ Load packages
from pyspark.sql.functions import col
import numpy as np
from pyspark.sql.types import IntegerType, DateType
import pyspark.sql.functions as fn
import shutil


##__ Path pointing to a results folder where outputs from this script will be exported, and where outputs from scripts 5 and 6 were exported.
result_dir=".../"


##__ Filtering option: Set parameters to same values as those used in script 6. 
## Since the script directly uses the temporary output produced in script 6, the filtering option used in script 6 is implicitly applied.
## !!! The same values must be specified here in order to ensure consistency in output file names.
# Filtering dummy: if 1 then a filter is applied.
filter_dummy = 1

# Filter name, used in the name of exported outputs.
filter_name=""


##__ Booleans indicating the types of time units to consider: weeks and/or half-months and/or months
run_week=False
run_halfMonth=True
run_month=False ## Note: Still under development and not functional yet. Leave to False.


##__ Specify manually the start and end time unit indices of the period to consider
# Week indices are integers of the form yyyyww (with ww in 01-53 according to ISO 8601)
if run_week:
    min_week=201301
    max_week=201401

# Half-month indices are integers of the form yyyyMM01 (for the first half of month MM of year yyyy) or yyyyMM16 (for the second half of month MM of year yyyy)
if run_halfMonth:
    min_halfMonth=20130101
    max_halfMonth=20131216

# Month indices are integers of the form yyyyMM
if run_month:
    min_month=201301
    max_month=201312


##__ Migration detection parameters
# Minimum duration for a meso-segment to be classified as a temporary migration event.
# Possibility to specify multiple values, in which case migration stock for these distinct values will be calculated and exported in separate files.
T_meso_min=[20,30,60] #in days

# Maximum observation gap allowed for consecutive observations at a single location to be grouped in the same meso-segment
eps_gap_meso=7 #in days


##__ Specify parameter sigma (minimum overlap between a meso-segment and a time unit to assign a migration status)
# Note: the value of sigma depends on the length of time units considered.
sigma_week=4
sigma_halfMonth=8
sigma_month=16


###
#1# Some useful functions
###


##__ Function calculating the start date of week 1 of a year
## arguments:
    # y: integer of the form yyyy

def StartDateOfWeekOfYear(y):
    if y in [2015,2026]:
        w1_start=str(y-1)+"-12-29"
    elif y in [2014,2020,2025]:
        w1_start=str(y-1)+"-12-30"
    elif y in [2013,2019]:
        w1_start=str(y-1)+"-12-31"
    elif y in [2018,2024]:
        w1_start=str(y)+"-01-01"
    elif y in [2012,2017,2023]:
        w1_start=str(y)+"-01-02"
    elif y in [2011,2022]:
        w1_start=str(y)+"-01-03"
    elif y in [2010,2016,2021]:
        w1_start=str(y)+"-01-04"
    return w1_start


##__ Functions to generate sequences of time units
# Full sequences of time units from 2010 to 2026
full_week_sequence = np.concatenate((np.array(list(range(201001, 201052+1, 1))),
                                     np.array(list(range(201101, 201152+1, 1))),
                                     np.array(list(range(201201, 201252+1, 1))),
                                     np.array(list(range(201301, 201352+1, 1))),
                                     np.array(list(range(201401, 201452+1, 1))),
                                     np.array(list(range(201501, 201553+1, 1))),
                                     np.array(list(range(201601, 201652+1, 1))),
                                     np.array(list(range(201701, 201752+1, 1))),
                                     np.array(list(range(201801, 201852+1, 1))),
                                     np.array(list(range(201901, 201952+1, 1))),
                                     np.array(list(range(202001, 202053+1, 1))),
                                     np.array(list(range(202101, 202152+1, 1))),
                                     np.array(list(range(202201, 202252+1, 1))),
                                     np.array(list(range(202301, 202352+1, 1))),
                                     np.array(list(range(202401, 202452+1, 1))),
                                     np.array(list(range(202501, 202552+1, 1))),
                                     np.array(list(range(202601, 202652+1, 1)))))
full_month_sequence = np.concatenate((np.array(list(range(201001, 201012+1, 1))),
                                      np.array(list(range(201101, 201112+1, 1))),
                                      np.array(list(range(201201, 201212+1, 1))),
                                      np.array(list(range(201301, 201312+1, 1))),
                                      np.array(list(range(201401, 201412+1, 1))),
                                      np.array(list(range(201501, 201512+1, 1))),
                                      np.array(list(range(201601, 201612+1, 1))),
                                      np.array(list(range(201701, 201712+1, 1))),
                                      np.array(list(range(201801, 201812+1, 1))),
                                      np.array(list(range(201901, 201912+1, 1))),
                                      np.array(list(range(202001, 202012+1, 1))),
                                      np.array(list(range(202101, 202112+1, 1))),
                                      np.array(list(range(202201, 202212+1, 1))),
                                      np.array(list(range(202301, 202312+1, 1))),
                                      np.array(list(range(202401, 202412+1, 1))),
                                      np.array(list(range(202501, 202512+1, 1))),
                                      np.array(list(range(202601, 202612+1, 1)))))
full_halfMonth_sequence = np.sort(np.concatenate((np.array([int(str(s)+"01") for s in list(full_month_sequence)]),
                                                  np.array([int(str(s)+"16") for s in list(full_month_sequence)]))))

# Function that extracts a sequence of week indices from the full sequence given start and end week indices specified
def generate_week_seq(start, end):
    if start == end:
        return list([start])
    else:
        output = full_week_sequence[np.where(full_week_sequence == start)[0][0]:(np.where(full_week_sequence == end)[0][0]+1)]
        return output.tolist()

# Same function for month indices
def generate_month_seq(start, end):
    if start == end:
        return list([start])
    else:
        output = np.array(list(range(start, end+1, 1)))
        output_filter = np.array([((int(str(w)[4:6]) < 13) & (int(str(w)[4:6]) > 0)) for w in output])
        output = output[output_filter]
        return output.tolist()

# Same function for half-month indices
def generate_halfmonth_seq(start, end):
    if start == end:
        return list([start])
    else:
        output = full_halfMonth_sequence[np.where(full_halfMonth_sequence == start)[0][0]:(np.where(full_halfMonth_sequence == end)[0][0]+1)]
        return output.tolist()


###
#2# Create full sequences of time units for the time period specified
###


if run_week:
    week_sequence=generate_week_seq(min_week, max_week)

if run_halfMonth:
    halfMonth_sequence=generate_halfmonth_seq(min_halfMonth, max_halfMonth)
  
if run_month:
    month_sequence=generate_month_seq(min_month,max_month)


###
#2# Import temporary user-segment DataFrame created in script 6
###


## Import and make sure columns have the correct type
UserSegment = spark.read.parquet(result_dir+"temp/UserSegment")

UserSegment=UserSegment\
    .select("device_id","start_date","end_date","origin_loc","destination_loc","date_closestObs_before","date_closestObs_after","loc_closestObs_before","loc_closestObs_after","minDuration")

for col_name in [value for value in UserSegment.columns if (value not in ["device_id", "start_date", "end_date", "date_closestObs_after", "date_closestObs_before"])]:
    UserSegment = UserSegment\
        .withColumn(col_name, col(col_name).cast(IntegerType()))

UserSegment = UserSegment\
    .withColumn("start_date", col("start_date").cast(DateType()))\
        .withColumn("end_date", col("end_date").cast(DateType()))\
            .withColumn("date_closestObs_after", col("date_closestObs_after").cast(DateType()))\
                .withColumn("date_closestObs_before", col("date_closestObs_before").cast(DateType()))


###
### Temporary migration stock by week
###

if run_week:
    # Loop on weeks and calculate a dummy equal to 1 if the segment implies that the user is in migration for the corresponding week
    for x in week_sequence:
        ## Start date of week 1 for the corresponding year (needed for calculating start date of week x)
        w1_start=StartDateOfWeekOfYear(int(str(x)[0:4]))
        
        
        ## Initialize temporary results for the time period by creating temporary constant columns in UserSegment giving the start and end date of week x
        UserSegment_temp=UserSegment\
            .withColumn("start_date_x",fn.date_add(fn.to_date(fn.lit(w1_start),"yyyy-M-d"),7*(int(str(x)[-2:])-1)))\
                .withColumn("end_date_x",fn.date_add("start_date_x",6))
                
        
        ## Calculate the number of days of week x intersected by each segment
        # Dummies indicating right, left or full overlap of the segment with week x
        UserSegment_temp=UserSegment_temp\
            .withColumn("overlap_left",fn.when((fn.datediff(col("end_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("end_date"))>=0) & (fn.datediff(col("start_date_x"),col("start_date"))>0),1).otherwise(0))\
                .withColumn("overlap_right",fn.when((fn.datediff(col("start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("start_date"))>=0) & (fn.datediff(col("end_date"),col("end_date_x"))>0),1).otherwise(0))\
                    .withColumn("overlap_full",fn.when((fn.datediff(col("start_date_x"),col("start_date"))>=0) & (fn.datediff(col("end_date"),col("end_date_x"))>=0),1).otherwise(0))
        
        # Number of days intersected
        UserSegment_temp=UserSegment_temp\
            .withColumn("N_daysIntersect_x",fn.when(col("overlap_right")==1,
                                                    fn.datediff(col("end_date_x"),col("start_date"))+1).otherwise(fn.when(col("overlap_left")==1,
                                                                                                                          fn.datediff(col("end_date"),col("start_date_x"))+1).otherwise(fn.when(col("overlap_full")==1,
                                                                                                                                                                                                fn.datediff(col("end_date_x"),col("start_date_x"))+1).otherwise(0))))
        
        
        ## Calculate the minimum date at which each segment could have started
        UserSegment_temp=UserSegment_temp\
            .withColumn("min_start_date",fn.when(col("loc_closestObs_before")!=col("destination_loc"),
                                               fn.date_add("date_closestObs_before",1)).otherwise(fn.when(col("date_closestObs_before").isNull(),
                                                                                                          col("start_date")).otherwise(fn.date_add("date_closestObs_before",eps_gap_meso+2))))
        
                                                                                                          
        ## Calculate the maximum date at which each segment could have ended
        UserSegment_temp=UserSegment_temp\
            .withColumn("max_end_date",fn.when(col("loc_closestObs_after")!=col("destination_loc"),
                                               fn.date_sub("date_closestObs_after",1)).otherwise(fn.when(col("date_closestObs_after").isNull(),
                                                                                                          col("end_date")).otherwise(fn.date_sub("date_closestObs_after",eps_gap_meso+2))))
        
                
        ## Loop on k_temp
        for k_temp in T_meso_min:
            ## Calculate dummy equal to 1 if the segment implies that the user is migrating in week x (high confidence, i.e. lower-bound estimate)
            UserSegment_temp=UserSegment_temp\
                .withColumn("TempMigrStatus_dummy_week_highConf_"+str(k_temp)+"days",fn.when((col("N_daysIntersect_x")>=sigma_week) & (col("minDuration")>=k_temp),1).otherwise(0))
            
            
            ## Calculate dummy equal to 1 if the segment implies that the user is migrating in week x (low confidence, i.e. upper-bound estimate)
            UserSegment_temp=UserSegment_temp\
                .withColumn("TempMigrStatus_dummy_week_lowConf_"+str(k_temp)+"days",fn.when((col("N_daysIntersect_x")>=sigma_week) & ((fn.datediff(col("max_end_date"),col("min_start_date"))+fn.lit(1))>=k_temp),1).otherwise(0))\

            
            ## Aggregate by origin-destination
            MigrationStock_ByOD_temp=UserSegment_temp\
                .groupBy("origin_loc","destination_loc")\
                    .agg(fn.sum("TempMigrStatus_dummy_week_highConf_"+str(k_temp)+"days").alias("N_migrant_highConf_"+str(x)),
                         fn.sum("TempMigrStatus_dummy_week_lowConf_"+str(k_temp)+"days").alias("N_migrant_lowConf_"+str(x)))
            
            
            ## Export
            MigrationStock_ByOD_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODWeek_"+str(x),header="true",mode="overwrite")
    
    
    ## Loop on migration duration threshold and Time units and join all results together
    for k_temp in T_meso_min:
        for x in week_sequence:
            ## Re-import migration stock estimates for value k_temp and time unit x
            MigrationStock_ByOD_temp=spark.read.csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODWeek_"+str(x),header="true",sep=";")
            
            ## Create time unit column
            MigrationStock_ByOD_temp=MigrationStock_ByOD_temp\
                .withColumn("WeekIndex",fn.lit(x))
            
            ## Rename migration estimate columns 
            MigrationStock_ByOD_temp=MigrationStock_ByOD_temp\
                .withColumnRenamed("N_migrant_highConf_"+str(x),"N_migrants_highConf")\
                    .withColumnRenamed("N_migrant_lowConf_"+str(x),"N_migrants_lowConf")\
                        .select("origin_loc","destination_loc","WeekIndex","N_migrants_highConf","N_migrants_lowConf")
                        
            ## Union
            if x==halfMonth_sequence[0]:
                MigrationStock_ByOD_final=MigrationStock_ByOD_temp.select("*")
            else:
                MigrationStock_ByOD_final=MigrationStock_ByOD_final\
                    .union(MigrationStock_ByOD_temp)
        
        ## Export final dataset for value k_temp
        MigrationStock_ByOD_final.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODWeek",header="true",mode="overwrite")
    
    
    ## Delete temporary outputs
    for k_temp in T_meso_min:
        for x in week_sequence:
            shutil.rmtree(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODWeek_"+str(x))


###
### Temporary migration stock by half-month
###


if run_halfMonth:
    # Loop on time units and calculate a dummy equal to 1 if the segment implies that the user is in migration for the corresponding half-month
    for x in halfMonth_sequence:
        ## Initialize temporary results for the time period by creating temporary constant columns in UserSegment giving the start and end date of half-month x
        UserSegment_temp=UserSegment\
            .withColumn("start_date_x",fn.to_date(fn.concat_ws("-",fn.lit(str(x)[0:4]),fn.lit(str(x)[4:6]),fn.lit(str(x)[6:8])),"yyyy-M-d"))
                    
        if str(x)[6:8]=="01":
            UserSegment_temp=UserSegment_temp\
                .withColumn("end_date_x",fn.date_add("start_date_x",14))
        elif str(x)[6:8]=="16":
            UserSegment_temp=UserSegment_temp\
                .withColumn("end_date_x",fn.last_day("start_date_x"))          
        
        
        ## Calculate the number of days of time unit x intersected by each segment
        # Dummies indicating right, left or full overlap of the segment with half-month x
        UserSegment_temp=UserSegment_temp\
            .withColumn("overlap_left",fn.when((fn.datediff(col("end_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("end_date"))>=0) & (fn.datediff(col("start_date_x"),col("start_date"))>0),1).otherwise(0))\
                .withColumn("overlap_right",fn.when((fn.datediff(col("start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("start_date"))>=0) & (fn.datediff(col("end_date"),col("end_date_x"))>0),1).otherwise(0))\
                    .withColumn("overlap_full",fn.when((fn.datediff(col("start_date_x"),col("start_date"))>=0) & (fn.datediff(col("end_date"),col("end_date_x"))>=0),1).otherwise(0))
        
        # Number of days intersected
        UserSegment_temp=UserSegment_temp\
            .withColumn("N_daysIntersect_x",fn.when(col("overlap_right")==1,
                                                    fn.datediff(col("end_date_x"),col("start_date"))+1).otherwise(fn.when(col("overlap_left")==1,
                                                                                                                          fn.datediff(col("end_date"),col("start_date_x"))+1).otherwise(fn.when(col("overlap_full")==1,
                                                                                                                                                                                                fn.datediff(col("end_date_x"),col("start_date_x"))+1).otherwise(0))))
        
        ## Calculate the minimum date at which each segment could have started
        UserSegment_temp=UserSegment_temp\
            .withColumn("min_start_date",fn.when(col("loc_closestObs_before")!=col("destination_loc"),
                                               fn.date_add("date_closestObs_before",1)).otherwise(fn.when(col("date_closestObs_before").isNull(),
                                                                                                          col("start_date")).otherwise(fn.date_add("date_closestObs_before",eps_gap_meso+2))))
        
                                                                                                          
        ## Calculate the maximum date at which each segment could have ended
        UserSegment_temp=UserSegment_temp\
            .withColumn("max_end_date",fn.when(col("loc_closestObs_after")!=col("destination_loc"),
                                               fn.date_sub("date_closestObs_after",1)).otherwise(fn.when(col("date_closestObs_after").isNull(),
                                                                                                          col("end_date")).otherwise(fn.date_sub("date_closestObs_after",eps_gap_meso+2))))
        
        
        ## Loop on k_temp
        for k_temp in T_meso_min:
            ## Calculate dummy equal to 1 if the segment implies that the user is migrating in half-month x (high confidence, i.e. lower-bound estimate)
            UserSegment_temp=UserSegment_temp\
                .withColumn("TempMigrStatus_dummy_halfMonth_highConf_"+str(k_temp)+"days",fn.when((col("N_daysIntersect_x")>=sigma_halfMonth) & (col("minDuration")>=k_temp),1).otherwise(0))
        

            ## Calculate dummy equal to 1 if the segment implies that the user is migrating in half-month x (low confidence, i.e. upper-bound estimate)
            UserSegment_temp=UserSegment_temp\
                .withColumn("TempMigrStatus_dummy_halfMonth_lowConf_"+str(k_temp)+"days",fn.when((col("N_daysIntersect_x")>=sigma_halfMonth) & ((fn.datediff(col("max_end_date"),col("min_start_date"))+fn.lit(1))>=k_temp),1).otherwise(0))

            
            ## Aggregate by origin-destination
            MigrationStock_ByOD_temp=UserSegment_temp\
                .groupBy("origin_loc","destination_loc")\
                    .agg(fn.sum("TempMigrStatus_dummy_halfMonth_highConf_"+str(k_temp)+"days").alias("N_migrant_highConf_"+str(x)),
                         fn.sum("TempMigrStatus_dummy_halfMonth_lowConf_"+str(k_temp)+"days").alias("N_migrant_lowConf_"+str(x)))
            
            
            ## Export in temporary folder
            MigrationStock_ByOD_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODHalfMonth_"+str(x),header="true",mode="overwrite")

    ## Loop on migration duration threshold and Time units and join all results together
    for k_temp in T_meso_min:
        for x in halfMonth_sequence:
            ## Re-import migration stock estimates for value k_temp and time unit x
            MigrationStock_ByOD_temp=spark.read.csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODHalfMonth_"+str(x),header="true",sep=";")
            
            ## Create time unit column
            MigrationStock_ByOD_temp=MigrationStock_ByOD_temp\
                .withColumn("HalfMonthIndex",fn.lit(x))
            
            ## Rename migration estimate columns 
            MigrationStock_ByOD_temp=MigrationStock_ByOD_temp\
                .withColumnRenamed("N_migrant_highConf_"+str(x),"N_migrants_highConf")\
                    .withColumnRenamed("N_migrant_lowConf_"+str(x),"N_migrants_lowConf")\
                        .select("origin_loc","destination_loc","HalfMonthIndex","N_migrants_highConf","N_migrants_lowConf")
                        
            ## Union
            if x==halfMonth_sequence[0]:
                MigrationStock_ByOD_final=MigrationStock_ByOD_temp.select("*")
            else:
                MigrationStock_ByOD_final=MigrationStock_ByOD_final\
                    .union(MigrationStock_ByOD_temp)
        
        ## Export final dataset for value k_temp
        MigrationStock_ByOD_final.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODHalfMonth",header="true",mode="overwrite")
    
    
    ## Delete temporary outputs
    for k_temp in T_meso_min:
        for x in halfMonth_sequence:
            shutil.rmtree(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationStock_"+str(k_temp)+"days_ByODHalfMonth_"+str(x))
       
            
            
##__ Delete UserSegment in temp
shutil.rmtree(result_dir+"temp/UserSegment")
