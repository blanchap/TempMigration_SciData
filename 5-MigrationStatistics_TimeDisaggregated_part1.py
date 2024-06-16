# Created on Thu Aug 10 17:41:20 2023

# @author: Paul Blanchard

##__ Description

# This script allows to calculate the number of users observed by origin and time unit for flow (departure and return) and stock (migration status).


###
#0# Preliminary
###

##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationStatistics_part1')\
    .config("spark.driver.memory", "10g")\
        .config("spark.executor.memory", "10g")\
            .config("spark.driver.maxResultSize", "5g")\
                .getOrCreate()


##__ Load packages
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
import numpy as np
import shutil


##__ Path pointing to the folder where outputs from script 1-4 were exported
out_dir=".../MigrationDetection_output/"


##__ Path pointing to a results folder where outputs from this script will be exported
result_dir=".../"


##__ Option to use migration detection output where a filter was applied to keep a subset of users, for instance to impose constraints on observational characteristics (e.g. length and frequency of observation).
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
    min_week=201401
    max_week=201601

# Half-month indices are integers of the form yyyyMM01 (for the first half of month MM of year yyyy) or yyyyMM16 (for the second half of month MM of year yyyy)
if run_halfMonth:
    min_halfMonth=20140101
    max_halfMonth=20151216

# Month indices are integers of the form yyyyMM
if run_month:
    min_month=201401
    max_month=201512


##__ Migration detection parameters
# Minimum duration for a meso-segment to be classified as a temporary migration event
T_meso_min=20 #in days

# Maximum observation gap allowed for consecutive observations at a single location to be grouped in the same meso-segment
eps_gap_meso=7 #in days

# Minimum fraction of days at location within a segment to be valid
Phi=0.5


##__ Specify tolerance parameter and parameter sigma (minimum overlap between a meso-segment and a time unit to assign a migration status)
# Note: the value of sigma depends on the length of time units considered.
eps_tol=7 #in days
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
#3# Import observation gap dataset
###


ObsGap=spark.read.parquet(out_dir+"UserObservationGap"+("_"+filter_name if filter_dummy==1 else ""))


###
#4# Build dataset with unique user-residence pairs and count the total number of users by origin location
###


##__ Import residence location datasets
## Import home locations for users with a unique residence
residLoc_unique=spark.read.parquet(out_dir+"UserUniqueResidence_data"+("_"+filter_name if filter_dummy==1 else ""))

## Import home locations by month for users with multiple residence
residLoc_multiple=spark.read.parquet(out_dir+"UserMultipleResidence_data"+("_"+filter_name if filter_dummy==1 else ""))


##__ Aggregate multiple residence location dataset to unique user-residence pairs
residLoc_multiple=residLoc_multiple\
    .select("device_id","resid_loc_multiple")\
        .dropDuplicates(["device_id","resid_loc_multiple"])


##__ Rename residence location columns and union
residLoc_unique=residLoc_unique\
    .withColumnRenamed("resid_loc_unique","origin_loc")

residLoc_multiple=residLoc_multiple\
    .withColumnRenamed("residLoc_multiple","origin_loc")

userResidencePairs=residLoc_unique\
    .union(residLoc_multiple)
    
##__ Calculate the total number of users by origin
usersByOrigin=userResidencePairs\
    .groupBy("origin_loc")\
        .count()\
            .withColumnRenamed("count","N_users")

usersByOrigin.write.mode("overwrite").parquet(result_dir+"temp/usersByOrigin_temp")
usersByOrigin=spark.read.parquet(result_dir+"temp/usersByOrigin_temp")


###
#5# Identify observational gaps that imply the non-observation of a user for migration DEPARTURE for any given week/month/half-month
###


##__ Time unit: week
if run_week:
    ## Loop on weeks and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that week
    for x in week_sequence:
        
        ## Start date of week 1 for the corresponding year (needed for calculating start date of week x)
        w1_start=StartDateOfWeekOfYear(int(str(x)[0:4]))
        
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week x
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.date_add(fn.to_date(fn.lit(w1_start),"yyyy-M-d"),7*(int(str(x)[-2:])-1)))\
                .withColumn("end_date_x",fn.date_add("start_date_x",6))
                
        
        ## Dummy equal to 1 if the observational gap implies a non-observation for week w
        # Dummy equal to 1 if the observational gap overlaps week w on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps week w on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if one of the 3 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= (eps_gap_meso+1), col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= (eps_gap_meso+1), col("maxNdaysAtLoc_temp")).otherwise(col("gapDuration")-(eps_gap_meso+1)+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
            
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) &
                                              (((col("NextSeg_loc")==col("NextSeg_resid_loc")) & (fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>=(T_meso_min)))  |
                                               ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (( (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("obsGap_start_date").isNull()) | ((col("PreviousSeg_loc")!=col("NextSeg_loc")) & (fn.datediff(col("start_date_x"),col("obsGap_start_date"))>eps_tol)))) |
                                                                                                  ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_left")
        
        # Dummy equal to 1 if one of the 3 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", col("maxDuration_withGap_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxNdaysAtLoc_temp2", col("maxNdaysAtLoc_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) &
                                               ((col("gapDuration")>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (((col("PreviousSeg_loc")!=col("NextSeg_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) | 
                                                                                                   ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) ))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_right")
        
        # Dummy equal to 1 if one of the 3 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= eps_gap_meso+1, col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= eps_gap_meso+1, col("maxNdaysAtLoc_temp")).otherwise(col("gapDuration")-(eps_gap_meso+1)+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("PreviousSeg_loc")!=col("NextSeg_loc")) | (col("obsGap_start_date").isNull()))) |
                                                                                                   ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) ))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")
        
        # Dummy equal to 1 if the observational gap implies a non-observation for week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_depart_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full")))\
                .drop("notObs_left","notObs_right","notObs_full")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        del ObsGap_temp

    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in week_sequence:
        ## Re-import results for week w
        notObs_depart=spark.read.parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_depart=notObs_depart\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_depart_"+str(x)).alias("notObs_depart_temp"))
        
        ## Aggregate by origin location
        notObs_depart=notObs_depart\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_depart_temp").alias("notObs_depart_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_depart,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_depart_temp"))\
                    .drop("notObs_depart_temp")
        
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginWeek_"+str(T_meso_min)+"days_depart",header="true",mode="overwrite")

    # Delete temporary file
    for x in week_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_week"+str(x))


##__ Time unit: half-month
if run_halfMonth:
    # Loop on half-months and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that half-month
    for x in halfMonth_sequence:
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week w
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.to_date(fn.concat_ws("-",fn.lit(str(x)[0:4]),fn.lit(str(x)[4:6]),fn.lit(str(x)[6:8])),"yyyy-M-d"))
                
        if str(x)[6:8]=="01":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.date_add("start_date_x",14))
        elif str(x)[6:8]=="16":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.last_day("start_date_x"))
                
                
        ## Dummy equal to 1 if the observational gap implies a non-observation for week w
        # Dummy equal to 1 if the observational gap overlaps week w on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps week w on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if one of the 3 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= (eps_gap_meso+1), col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= (eps_gap_meso+1), col("maxNdaysAtLoc_temp")).otherwise(col("gapDuration")-(eps_gap_meso+1)+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
            
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) &
                                              (((col("NextSeg_loc")==col("NextSeg_resid_loc")) & (fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>=(T_meso_min)))  |
                                               ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (( (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("obsGap_start_date").isNull()) | ((col("PreviousSeg_loc")!=col("NextSeg_loc")) & (fn.datediff(col("start_date_x"),col("obsGap_start_date"))>eps_tol)))) |
                                                                                                  ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_left")
        
        # Dummy equal to 1 if one of the 3 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", col("maxDuration_withGap_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxNdaysAtLoc_temp2", col("maxNdaysAtLoc_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) &
                                               ((col("gapDuration")>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (((col("PreviousSeg_loc")!=col("NextSeg_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) | 
                                                                                                   ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) ))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_right")
        
        # Dummy equal to 1 if one of the 3 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= eps_gap_meso+1, col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("start_date_x"), col("obsGap_start_date")) >= eps_gap_meso+1, col("maxNdaysAtLoc_temp")).otherwise(col("gapDuration")-(eps_gap_meso+1)+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("PreviousSeg_loc")!=col("NextSeg_loc")) | (col("obsGap_start_date").isNull()))) |
                                                                                                   ((col("PreviousSeg_loc")==col("NextSeg_loc")) & (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) ))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")

        # Dummy equal to 1 if the observational gap implies a non-observation for half-month h
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_depart_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full")))\
                .drop("notObs_left","notObs_right","notObs_full")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        del ObsGap_temp

    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in halfMonth_sequence:
        ## Re-import results for half-month h
        notObs_depart=spark.read.parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_depart=notObs_depart\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_depart_"+str(x)).alias("notObs_depart_temp"))
        
        ## Aggregate by origin location
        notObs_depart=notObs_depart\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_depart_temp").alias("notObs_depart_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_depart,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_depart_temp"))\
                    .drop("notObs_depart_temp")
        
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginHalfMonth_"+str(T_meso_min)+"days_depart",header="true",mode="overwrite")

    # Delete temporary file
    for x in halfMonth_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))


###
#6# Count the number of users observed for migration RETURN estimation for any given week/month/half-month
###


##__ Time unit: week
if run_week:
    # Loop on weeks and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that week
    for x in week_sequence:
        
        ## Start date of week 1 for the corresponding year (needed for calculating start date of week w)
        w1_start=StartDateOfWeekOfYear(int(str(x)[0:4]))
        
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week w
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.date_add(fn.to_date(fn.lit(w1_start),"yyyy-M-d"),7*(int(str(x)[-2:])-1)))\
                .withColumn("end_date_x",fn.date_add("start_date_x",6))
                
        
        ## Dummy equal to 1 if the observational gap implies a non-observation for week w
        # Dummy equal to 1 if the observational gap overlaps week w on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps week w on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if one of the 3 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", col("maxDuration_withGap_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxNdaysAtLoc_temp2", col("maxNdaysAtLoc_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
            
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) &
                                              ((col("gapDuration")>=T_meso_min) |
                                               (col("obsGap_start_date").isNull()) |
                                               ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & (col("NextSeg_loc")!=col("PreviousSeg_loc"))) |
                                                                                                          ((col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi) & (col("NextSeg_loc")==col("PreviousSeg_loc")) & ((fn.datediff(col("NextSeg_start_date"),col("start_date_x")))>(eps_gap_meso+1)))))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_left")
        
        # Dummy equal to 1 if one of the 3 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date")))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= (eps_gap_meso+1), col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= (eps_gap_meso+1), col("maxNdaysAtLoc_temp")).otherwise(fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration")-(eps_gap_meso+1)))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) &
                                               ((((fn.datediff(col("end_date_x"),col("PreviousSeg_end_date")))>=T_meso_min) & (col("PreviousSeg_loc")==col("PreviousSeg_resid_loc"))) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & (((col("NextSeg_loc")!=col("PreviousSeg_loc")) & (fn.datediff(col("obsGap_end_date"),col("end_date_x"))>eps_tol)) | (col("obsGap_end_date").isNull()))) |
                                                                                                           ((col("NextSeg_loc")==col("PreviousSeg_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))) )),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_right")
        
        # Dummy equal to 1 if one of the 3 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date")))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= eps_gap_meso+1, col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= eps_gap_meso+1, col("maxNdaysAtLoc_temp")).otherwise(fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration")-(eps_gap_meso+1)))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((col("obsGap_start_date").isNull())|
                                                (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>=T_meso_min) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("NextSeg_loc")!=col("PreviousSeg_loc")) | (col("obsGap_end_date").isNull()))) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("NextSeg_loc")==col("PreviousSeg_loc")) & (fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")
        
        # Dummy equal to 1 if the observational gap implies a non-observation for week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_return_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full")))\
                .drop("notObs_left","notObs_right","notObs_full")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        del ObsGap_temp


    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in week_sequence:
        ## Re-import results for week w
        notObs_return=spark.read.parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_return=notObs_return\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_return_"+str(x)).alias("notObs_return_temp"))
        
        ## Aggregate by origin location
        notObs_return=notObs_return\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_return_temp").alias("notObs_return_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_return,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_return_temp"))\
                    .drop("notObs_return_temp")
        
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginWeek_"+str(T_meso_min)+"days_return",header="true",mode="overwrite")


    # Delete temporary file
    for x in week_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_week"+str(x))


##__ Time unit: half-month
if run_halfMonth:
    # Loop on half-months and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that half-month
    for x in halfMonth_sequence:
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week w
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.to_date(fn.concat_ws("-",fn.lit(str(x)[0:4]),fn.lit(str(x)[4:6]),fn.lit(str(x)[6:8])),"yyyy-M-d"))
                
        if str(x)[6:8]=="01":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.date_add("start_date_x",14))
        elif str(x)[6:8]=="16":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.last_day("start_date_x"))
                
                
        ## Dummy equal to 1 if the observational gap implies a non-observation for week w
        # Dummy equal to 1 if the observational gap overlaps week w on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps week w on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if one of the 3 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", col("maxDuration_withGap_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxNdaysAtLoc_temp2", col("maxNdaysAtLoc_temp")-fn.lit(eps_gap_meso+1))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
            
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) &
                                              ((col("gapDuration")>=T_meso_min) |
                                               (col("obsGap_start_date").isNull()) |
                                               ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & (col("NextSeg_loc")!=col("PreviousSeg_loc"))) |
                                                                                                          ((col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi) & (col("NextSeg_loc")==col("PreviousSeg_loc")) & ((fn.datediff(col("NextSeg_start_date"),col("start_date_x")))>(eps_gap_meso+1)))))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_left")
        
        # Dummy equal to 1 if one of the 3 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date")))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= (eps_gap_meso+1), col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= (eps_gap_meso+1), col("maxNdaysAtLoc_temp")).otherwise(fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration")-(eps_gap_meso+1)))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) &
                                               ((((fn.datediff(col("end_date_x"),col("PreviousSeg_end_date")))>=T_meso_min) & (col("PreviousSeg_loc")==col("PreviousSeg_resid_loc"))) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (((col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & (((col("NextSeg_loc")!=col("PreviousSeg_loc")) & (fn.datediff(col("obsGap_end_date"),col("end_date_x"))>eps_tol)) | (col("obsGap_end_date").isNull()))) |
                                                                                                           ((col("NextSeg_loc")==col("PreviousSeg_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))) )),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_right")
        
        # Dummy equal to 1 if one of the 3 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date")))\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= eps_gap_meso+1, col("maxDuration_withGap_temp")).otherwise(fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before"))-(eps_gap_meso+1)))\
            .withColumn("maxNdaysAtLoc_temp2", fn.when(fn.datediff(col("obsGap_end_date"), col("end_date_x")) >= eps_gap_meso+1, col("maxNdaysAtLoc_temp")).otherwise(fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration")-(eps_gap_meso+1)))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")

        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((col("obsGap_start_date").isNull())|
                                                (fn.datediff(col("end_date_x"),col("PreviousSeg_end_date"))>=T_meso_min) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi) & ((col("NextSeg_loc")!=col("PreviousSeg_loc")) | (col("obsGap_end_date").isNull()))) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("NextSeg_loc")==col("PreviousSeg_loc")) & (fn.datediff(col("NextSeg_start_date"),col("start_date_x"))>(eps_gap_meso+1)) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")
        
        # Dummy equal to 1 if the observational gap implies a non-observation for half-month h
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_return_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full")))\
                .drop("notObs_left","notObs_right","notObs_full")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        del ObsGap_temp

    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in halfMonth_sequence:
        ## Re-import results for half-month h
        notObs_return=spark.read.parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_return=notObs_return\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_return_"+str(x)).alias("notObs_return_temp"))
        
        ## Aggregate by origin location
        notObs_return=notObs_return\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_return_temp").alias("notObs_return_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_return,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_return_temp"))\
                    .drop("notObs_return_temp")
        
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginHalfMonth_"+str(T_meso_min)+"days_return",header="true",mode="overwrite")

    # Delete temporary file
    for x in halfMonth_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))


###
#7# Count the number of users observed for migration STOCK estimation for any given week/month/half-month
###


##__ Time unit: week
if run_week:
    # Loop on weeks and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that week
    for x in week_sequence:
        
        ## Start date of week 1 for the corresponding year (needed for calculating start date of week w)
        w1_start=StartDateOfWeekOfYear(int(str(x)[0:4]))
        
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week w
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.date_add(fn.to_date(fn.lit(w1_start),"yyyy-M-d"),7*(int(str(x)[-2:])-1)))\
                .withColumn("end_date_x",fn.date_add("start_date_x",6))
                
        
        ## Dummy equal to 1 if the observational gap implies a non-observation for time unit x
        # Dummy equal to 1 if the observational gap overlaps time unit x on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps time unit x on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers time unit x
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if the observational gap covers time unit x
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_within",fn.when((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>0) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)))
        
        # Dummy equal to 1 if one of the 4 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))<sigma_week) &
                                              ((col("gapDuration")>=T_meso_min) |
                                               (col("obsGap_start_date").isNull()) |
                                               ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                               ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("overlap_left","maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2")
        
        # Dummy equal to 1 if one of the 4 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("obsGap_start_date"))+1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp2", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) & (fn.datediff(col("obsGap_start_date"),col("start_date_x"))<sigma_week) &
                                               ((col("gapDuration")>=T_meso_min)|
                                                (col("obsGap_end_date").isNull())|
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi))),1).otherwise(0))\
                .drop("overlap_right","maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2")
        
        # Dummy equal to 1 if one of the 5 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date"))+1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((col("gapDuration")>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("obsGap_start_date").isNull())) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")
        
        # Dummy equal to 1 if one of the 4 cases for within overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .withColumn("maxDuration_withGap_temp3", fn.datediff(col("NextSeg_end_date"), col("obsGap_start_date"))+1)\
            .withColumn("maxDensity_temp3", (col("gapDuration")+col("NextSeg_N_daysAtLoc"))/col("maxDuration_withGap_temp3"))\
            .withColumn("maxDuration_withGap_temp4", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_start_date"))+1)\
            .withColumn("maxDensity_temp4", (col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))/col("maxDuration_withGap_temp4"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_within",fn.when((col("overlap_within")==1) & (fn.datediff(col("obsGap_start_date"),col("start_date_x"))<sigma_week) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))<sigma_week) &
                                               (((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("NextSeg_date_closestObs_after").isNull()) & (col("maxDuration_withGap_temp3")>=T_meso_min) & (col("maxDensity_temp3")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("PreviousSeg_date_closestObs_before").isNull()) & (col("maxDuration_withGap_temp4")>=T_meso_min) & (col("maxDensity_temp4")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","maxDuration_withGap_temp3","maxDensity_temp3","maxDuration_withGap_temp4","maxDensity_temp4","overlap_within")
        
        
        # Dummy equal to 1 if the observational gap implies a non-observation for week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_status_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full"),col("notObs_within")))\
                .drop("notObs_left","notObs_right","notObs_full","notObs_within")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        del ObsGap_temp


    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in week_sequence:
        ## Re-import results for week w
        notObs_status=spark.read.parquet(result_dir+"temp/notObserved_ByGap_week"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_status=notObs_status\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_status_"+str(x)).alias("notObs_status_temp"))
        
        ## Aggregate by origin location
        notObs_status=notObs_status\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_status_temp").alias("notObs_status_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_status,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_status_temp"))\
                    .drop("notObs_status_temp")
    
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginWeek_"+str(T_meso_min)+"days_status",header="true",mode="overwrite")

    # Delete temporary file
    for x in week_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_week"+str(x))


##__ Time unit: half-month
if run_halfMonth:
    # Loop on weeks and calculate a dummy equal to 1 if the observational gap implies a non-observation of the corresponding user for that week
    for x in halfMonth_sequence:
        
        ## Initialize temporary results for the time period by creating temporary constant columns in ObsGap giving the start and end date of week w
        ObsGap_temp=ObsGap\
            .withColumn("start_date_x",fn.to_date(fn.concat_ws("-",fn.lit(str(x)[0:4]),fn.lit(str(x)[4:6]),fn.lit(str(x)[6:8])),"yyyy-M-d"))
                
        if str(x)[6:8]=="01":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.date_add("start_date_x",14))
        elif str(x)[6:8]=="16":
            ObsGap_temp=ObsGap_temp\
                .withColumn("end_date_x",fn.last_day("start_date_x"))
                
        
        ## Dummy equal to 1 if the observational gap implies a non-observation for time unit x
        # Dummy equal to 1 if the observational gap overlaps time unit x on the left
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_left",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>0) | (col("obsGap_start_date").isNull())) & (fn.datediff(col("obsGap_end_date"),col("start_date_x"))>=-1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0)),1).otherwise(0))

        # Dummy equal to 1 if the observational gap overlaps time unit x on the right
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_right",fn.when(((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>=0) & (fn.datediff(col("end_date_x"),col("obsGap_start_date"))>=0) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
            
        # Dummy equal to 1 if the observational gap covers time unit x
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_full",fn.when((((fn.datediff(col("start_date_x"),col("obsGap_start_date"))>=-1) | (col("obsGap_start_date").isNull())) & ((fn.datediff(col("obsGap_end_date"),col("end_date_x"))>=0) | (col("obsGap_end_date").isNull()))),1).otherwise(0))
        
        # Dummy equal to 1 if the observational gap covers time unit x
        ObsGap_temp=ObsGap_temp\
            .withColumn("overlap_within",fn.when((fn.datediff(col("obsGap_start_date"),col("start_date_x"))>0) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))>0),1).otherwise(0))
        
        # Dummy equal to 1 if one of the 4 cases for left overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_left",fn.when((col("overlap_left")==1) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))<sigma_halfMonth) &
                                              ((col("gapDuration")>=T_meso_min) |
                                               (col("obsGap_start_date").isNull()) |
                                               ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                               ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("overlap_left","maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2")
        
        # Dummy equal to 1 if one of the 4 cases for right overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("obsGap_start_date"))+1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp2", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_right",fn.when((col("overlap_right")==1) & (fn.datediff(col("obsGap_start_date"),col("start_date_x"))<sigma_halfMonth) &
                                               ((col("gapDuration")>=T_meso_min)|
                                                (col("obsGap_end_date").isNull())|
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi))),1).otherwise(0))\
                .drop("overlap_right","maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2")
        
        # Dummy equal to 1 if one of the 5 cases for full overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("end_date_x"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+fn.datediff(col("end_date_x"), col("PreviousSeg_end_date"))+1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("NextSeg_date_closestObs_after"), col("start_date_x")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("NextSeg_start_date"), col("start_date_x"))+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_full",fn.when((col("overlap_full")==1) &
                                               ((col("gapDuration")>=T_meso_min) |
                                                (col("obsGap_end_date").isNull()) |
                                                ((col("obsGap_start_date").isNull())) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","overlap_full")
        
        # Dummy equal to 1 if one of the 4 cases for within overlap is satisfied
        ObsGap_temp = ObsGap_temp\
            .withColumn("maxDuration_withGap_temp", fn.datediff(col("NextSeg_date_closestObs_after"), col("obsGap_start_date")))\
            .withColumn("maxNdaysAtLoc_temp", col("gapDuration")+col("NextSeg_N_daysAtLoc")+fn.datediff(col("NextSeg_date_closestObs_after"), col("NextSeg_end_date"))-1)\
            .withColumn("maxDensity_temp", col("maxNdaysAtLoc_temp")/col("maxDuration_withGap_temp"))\
            .withColumn("maxDuration_withGap_temp2", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_date_closestObs_before")))\
            .withColumn("maxNdaysAtLoc_temp2", fn.datediff(col("PreviousSeg_start_date"), col("PreviousSeg_date_closestObs_before"))-1+col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))\
            .withColumn("maxDensity_temp2", col("maxNdaysAtLoc_temp2")/col("maxDuration_withGap_temp2"))\
            .withColumn("maxDuration_withGap_temp3", fn.datediff(col("NextSeg_end_date"), col("obsGap_start_date"))+1)\
            .withColumn("maxDensity_temp3", (col("gapDuration")+col("NextSeg_N_daysAtLoc"))/col("maxDuration_withGap_temp3"))\
            .withColumn("maxDuration_withGap_temp4", fn.datediff(col("obsGap_end_date"), col("PreviousSeg_start_date"))+1)\
            .withColumn("maxDensity_temp4", (col("PreviousSeg_N_daysAtLoc")+col("gapDuration"))/col("maxDuration_withGap_temp4"))\
            .drop("maxNdaysAtLoc_temp", "maxNdaysAtLoc_temp2")
        
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_within",fn.when((col("overlap_within")==1) & (fn.datediff(col("obsGap_start_date"),col("start_date_x"))<sigma_halfMonth) & (fn.datediff(col("end_date_x"),col("obsGap_end_date"))<sigma_halfMonth) &
                                               (((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("maxDuration_withGap_temp")>=T_meso_min) & (col("maxDensity_temp")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("maxDuration_withGap_temp2")>=T_meso_min) & (col("maxDensity_temp2")>=Phi)) |
                                                ((col("NextSeg_loc")!=col("NextSeg_resid_loc")) & (col("NextSeg_date_closestObs_after").isNull()) & (col("maxDuration_withGap_temp3")>=T_meso_min) & (col("maxDensity_temp3")>=Phi)) |
                                                ((col("PreviousSeg_loc")!=col("PreviousSeg_resid_loc")) & (col("PreviousSeg_date_closestObs_before").isNull()) & (col("maxDuration_withGap_temp4")>=T_meso_min) & (col("maxDensity_temp4")>=Phi))),1).otherwise(0))\
                .drop("maxDuration_withGap_temp","maxDensity_temp","maxDuration_withGap_temp2","maxDensity_temp2","maxDuration_withGap_temp3","maxDensity_temp3","maxDuration_withGap_temp4","maxDensity_temp4","overlap_within")
        
        
        # Dummy equal to 1 if the observational gap implies a non-observation for week w
        ObsGap_temp=ObsGap_temp\
            .withColumn("notObs_status_"+str(x),fn.greatest(col("notObs_left"),col("notObs_right"),col("notObs_full"),col("notObs_within")))\
                .drop("notObs_left","notObs_right","notObs_full","notObs_within")
        
        
        ## Export in temporary folder
        ObsGap_temp.write.mode("overwrite").parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        del ObsGap_temp


    # Loop on week to re-import results and aggregate by user-residence and then by origin location
    usersByOrigin_temp=usersByOrigin.select("*")
    for x in halfMonth_sequence:
        ## Re-import results for week w
        notObs_status=spark.read.parquet(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))
        
        
        ## Aggregate by user-residence
        notObs_status=notObs_status\
            .groupBy("device_id","origin_loc")\
                .agg(fn.max("notObs_status_"+str(x)).alias("notObs_status_temp"))
        
        ## Aggregate by origin location
        notObs_status=notObs_status\
            .groupBy("origin_loc")\
                .agg(fn.sum("notObs_status_temp").alias("notObs_status_temp"))

        
        ## Join to table with total number of users by origin and infer the number of users effectively observed on week w
        usersByOrigin_temp=usersByOrigin_temp\
            .join(notObs_status,on=["origin_loc"],how="leftouter")\
                .withColumn("N_users_observed_"+str(x),col("N_users")-col("notObs_status_temp"))\
                    .drop("notObs_status_temp")
        
    # Export
    usersByOrigin_temp.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/N_users_ByOriginHalfMonth_"+str(T_meso_min)+"days_status",header="true",mode="overwrite")

    # Delete temporary file
    for x in halfMonth_sequence:
        shutil.rmtree(result_dir+"temp/notObserved_ByGap_halfMonth"+str(x))


## Delete remaining folders in temp
shutil.rmtree(result_dir+"temp/usersByOrigin_temp")
