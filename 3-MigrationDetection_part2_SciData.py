# Created on Mon May 20 17:38:39 2024

#@author: Paul Blanchard

##__ Description

# MigrationDetection_part2 proceeds with the meso-segment detection procedure.


###
#0# Preliminary
###


##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationDetection_part2')\
    .config("spark.driver.memory", "15g")\
    .config("spark.executor.memory", "15g")\
    .config("spark.driver.maxResultSize", "5g")\
    .getOrCreate()


##__ Load packages
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql import Window
import sys
import shutil


##__ Path pointing to the folder where the daily locations dataset was exported (Script MigrationDetection_part1.py)
out_dir=".../MigrationDetection_output/"


##__ Option to use filtered input that keeps a subset of users, for instance to impose constraints on observational characteristics (e.g. length and frequency of observation).
# Filtering dummy: if 1 then a filter is applied.
filter_dummy = 1

# Filter name, used in the name of exported outputs.
filter_name=""


##__ Migration detection parameters
# Minimum duration for a meso-segment to be classified as a temporary migration event
T_meso_min=20 #in days

# Maximum observation gap allowed for consecutive observations at a single location to be grouped in the same meso-segment
eps_gap_meso=7 #in days

# Minimum fraction of days at location within a segment to be valid
Phi=0.5


###
#1# Import the dataset of daily locations (calculated in MigrationDetection_part0.py)
###


daily_loc=spark.read.parquet(out_dir+"dailyLoc_data"+("_"+filter_name if filter_dummy==1 else ""))


###
#2# Import residence locations datasets
###


##__ Import home locations for users with a unique residence
residLoc_unique=spark.read.parquet(out_dir+"UserUniqueResidence_data"+("_"+filter_name if filter_dummy==1 else ""))


##__ Import home locations by month for users with multiple residence
residLoc_multiple=spark.read.parquet(out_dir+"UserMultipleResidence_data"+("_"+filter_name if filter_dummy==1 else ""))

    
###
#3# Temporary migration event detection
###


##__ Detect groups of contiguous daily locations
# Re-create date column in daily_loc from year, month and day columns
daily_loc=daily_loc\
    .withColumn("date",fn.to_date(fn.concat_ws("-",col("year"),col("month"),col("day")),format="yyyy-M-d"))

# Calculate lag daily location and lag date
w_user=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("date").cast("timestamp").cast("long")))

daily_loc=daily_loc\
    .withColumn("daily_loc_lag",fn.lag("daily_loc").over(w_user))\
        .withColumn("date_lag",fn.lag("date").over(w_user))

# Calculate a dummy equal to 1 if there is a change in the daily location OR a gap greater than eps_gap_meso relative to the previous observation, i.e. a new group starts.
daily_loc=daily_loc\
    .withColumn("LocSegment_dummy",fn.when((col("daily_loc")!=col("daily_loc_lag")) | ((fn.datediff(col("date"),col("date_lag"))-1)>eps_gap_meso),1).otherwise(0))

# Calculate a daily location group index
w_cumsum=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("date").cast("timestamp").cast("long"))\
           .rowsBetween(-sys.maxsize, 0))

daily_loc=daily_loc\
    .withColumn("LocSegment_index",fn.sum("LocSegment_dummy").over(w_cumsum))\
        .withColumn("LocSegment_index",col("LocSegment_index").cast("int")+1)

# Drop columns that will no longer be used
daily_loc=daily_loc\
    .drop("LocSegment_dummy","daily_loc_lag","date_lag")


##__ Join residence location to daily locations and define the macro-location at the level of daily location groups
# Join
daily_loc=daily_loc\
    .join(residLoc_unique,on="device_id",how="leftouter")

daily_loc=daily_loc\
    .join(residLoc_multiple,on=["device_id","month","year"],how="leftouter")

# Separate data for users with multiple and unique residence locations
daily_loc_unique=daily_loc\
    .filter(col("resid_loc_unique")>0)
        
daily_loc_multi=daily_loc\
    .filter(col("resid_loc_multiple")>0)

# Group-level residence location for users with a unique residence location is non-ambiguous
daily_loc_unique=daily_loc_unique\
    .withColumn("resid_loc_seg",col("resid_loc_unique"))\
        .drop("resid_loc_unique","resid_loc_multiple")

# For users with multiple residence locations: Calculate residence location by group as the within-segment modal residence location, for potential cases where a macro-movement date intersects a daily location group
w_resid_multi=Window.partitionBy("device_id","LocSegment_index")

resid_loc_seg_multi=daily_loc_multi\
    .select("device_id","date","LocSegment_index","resid_loc_multiple")\
        .groupBy("device_id","LocSegment_index","resid_loc_multiple")\
            .agg(fn.count(fn.lit(1)).alias("N_daysAtResid"),fn.min("date").alias("min_date"))\
                .withColumn("N_daysAtResid_max",fn.max("N_daysAtResid").over(w_resid_multi))\
                    .where((col("N_daysAtResid")==col("N_daysAtResid_max")))\
                        .withColumn("LatestResid_date",fn.max("min_date").over(w_resid_multi))\
                            .where((col("min_date")==col("LatestResid_date")))\
                                .select("device_id","LocSegment_index",col("resid_loc_multiple").alias("resid_loc_seg"))

daily_loc_multi=daily_loc_multi\
    .join(resid_loc_seg_multi,on=["device_id","LocSegment_index"],how="leftouter")


daily_loc_multi=daily_loc_multi\
    .drop("resid_loc_unique","resid_loc_multiple")\
        .select("device_id","date","year","month","day","daily_loc","LocSegment_index","resid_loc_seg")
    
# Union daily_loc_unique and daily_loc_multi
daily_loc_unique=daily_loc_unique\
    .select("device_id","day","month","year","date","daily_loc","LocSegment_index","resid_loc_seg")

daily_loc_multi=daily_loc_multi\
    .select("device_id","day","month","year","date","daily_loc","LocSegment_index","resid_loc_seg")

daily_loc=daily_loc_unique.union(daily_loc_multi)


## Aggregate by user and group index and for each unit calculate: start date, end date, segment location, home location 
loc_seg=daily_loc\
    .groupBy("device_id","LocSegment_index")\
        .agg(fn.min("date").alias("start_date"),
             fn.max("date").alias("end_date"),
             fn.first("daily_loc").alias("seg_loc"),
             fn.last("resid_loc_seg").alias("resid_loc_seg"))


##__ Merge groups at the same location if they are less than eps_gap_meso days appart
    ## Such groups may be consecutive -- in which case the user is simply
    ## unobserved between merged groups -- or non-consecutive -- in which case 
    ## the user is observed at different locations where she stayed less than eps_gap_meso days.
    ## This can lead to overlap between merged groups, an issue that is dealt with in the next step.

# Partitition by user and location and calculate lag end date (i.e. end date of previous segment AT THE SAME LOCATION)
w_user_loc=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"))\
       .orderBy(col("LocSegment_index")))

loc_seg=loc_seg\
    .withColumn("end_date_SameLocLag",fn.lag("end_date").over(w_user_loc))

# Calculate the time elapsed since the end of the previous group at the same location
loc_seg=loc_seg\
    .withColumn("TimeElapsed_segLag",fn.datediff(col("start_date"),col("end_date_SameLocLag"))-1)\
        .drop("end_date_SameLocLag")

# Calculate a dummy equal to 1 when the time elapsed since the end of the previous group at the same location exceeded eps_gap_meso days, i.e. the group is not merged with the previous group at the same location.
loc_seg=loc_seg\
    .withColumn("eps_exceed_dummy",fn.when(col("TimeElapsed_segLag")>eps_gap_meso,1).otherwise(0))\
        .drop("TimeElapsed_segLag")

# Calculate a sub-index that is constant within user-location-group
w_cumsum2=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"))\
       .orderBy(col("LocSegment_index"))\
           .rowsBetween(-sys.maxsize, 0))

loc_seg=loc_seg\
    .withColumn("LocSegment_subIndex",fn.sum("eps_exceed_dummy").over(w_cumsum2))\
        .drop("eps_exceed_dummy")

# Calculate a new index allowing to merge groups
w_merge=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"),col("LocSegment_subIndex"))\
       .orderBy(col("LocSegment_index")))

loc_seg=loc_seg\
    .withColumn("LocSegment_index_merge",fn.min(col("LocSegment_index")).over(w_merge))\
        .drop("LocSegment_subIndex")
    
# Calculate the updated start date and end date of merged groups
w_user_seg=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("LocSegment_index_merge")))

loc_seg=loc_seg\
    .withColumn("start_date_merge",fn.min(col("start_date")).over(w_user_seg))\
        .withColumn("end_date_merge",fn.max(col("end_date")).over(w_user_seg))

# Calculate lag segment end date and lead segment start date (used to calculate upper bound duration, also reused later on final merged segments)
w_laglead=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("LocSegment_index")))

loc_seg=loc_seg\
    .withColumn("end_date_lag",fn.lag("end_date").over(w_laglead))\
        .withColumn("start_date_lead",fn.lead("start_date").over(w_laglead))

# Calculate lag and lead segment location
loc_seg=loc_seg\
    .withColumn("seg_loc_lag",fn.lag("seg_loc").over(w_laglead))\
        .withColumn("seg_loc_lead",fn.lead("seg_loc").over(w_laglead))


##__ Identify and keep segments that satisfy the minimum density criterion (Phi)
# Calculate the length of each segment (in days)
loc_seg=loc_seg\
    .withColumn("segLength",fn.datediff(col("end_date"),col("start_date"))+1)

# Calculate the length of merged segments (equivalent to lower bound duration)
loc_seg=loc_seg\
    .withColumn("mergedSegLength",fn.datediff(col("end_date_merge"),col("start_date_merge"))+1)

# Calculate the total number of days at the merged segment location within the merged segment
loc_seg=loc_seg\
    .withColumn("N_daysAtLoc",fn.sum("segLength").over(w_user_seg))

# Calculate the proportion of days at the merged segment location
loc_seg=loc_seg\
    .withColumn("prop_daysAtLoc",col("N_daysAtLoc")/col("mergedSegLength"))\
        .drop("N_daysAtLoc")

# Keep segments with a proportion of days of at least Phi
loc_seg=loc_seg\
    .filter(col("prop_daysAtLoc")>=Phi)


##__ Aggregate at the level of merged groups
resid_BySegMerge=loc_seg\
    .groupBy("device_id","LocSegment_index_merge","resid_loc_seg")\
        .agg(fn.sum("segLength").alias("N_daysAtResid"))\
            .withColumn("N_daysAtResid_max",fn.max("N_daysAtResid").over(w_user_seg))\
                .where((col("N_daysAtResid")==col("N_daysAtResid_max")))\
                    .withColumn("resid_loc_seg_max",fn.max("resid_loc_seg").over(w_user_seg))\
                        .dropDuplicates(["device_id","LocSegment_index_merge"])\
                            .drop("N_daysAtResid","N_daysAtResid_max","resid_loc_seg")\
                                .withColumnRenamed("resid_loc_seg_max","resid_loc_seg")
        
loc_seg_merge=loc_seg\
    .groupBy("device_id","resid_loc_seg","LocSegment_index_merge")\
        .agg(fn.first("seg_loc").alias("merged_seg_loc"),
             fn.first("start_date_merge").alias("start_date_merge"),
             fn.first("end_date_merge").alias("end_date_merge"),
             fn.min("end_date_lag").alias("date_closestObs_before"),
             fn.max("start_date_lead").alias("date_closestObs_after"))
  
loc_seg_merge=loc_seg_merge\
    .join(resid_BySegMerge,on=["device_id","resid_loc_seg","LocSegment_index_merge"],how="left")
    

##__ Export in temporary folder and re-import for optimization
loc_seg_merge.write.mode("overwrite").parquet(out_dir+"temp/loc_seg_merge")
loc_seg_merge=spark.read.parquet(out_dir+"temp/loc_seg_merge")


##__ Calculate lag and lead location for each merged segment separately and join to loc_seg_merge
# Keep rows of loc_seg corresponding to the minimum or maximum start/end date for each user-merged segment group
LagLeadLoc=loc_seg\
    .select("device_id","start_date","end_date","LocSegment_index_merge","seg_loc_lag","seg_loc_lead")\
        .withColumn("isMinStartDate",fn.when(col("start_date")==fn.min("start_date").over(w_user_seg),1).otherwise(0))\
            .withColumn("isMaxEndDate",fn.when(col("end_date")==fn.max("end_date").over(w_user_seg),1).otherwise(0))\
                .filter((col("isMinStartDate")==1) | (col("isMaxEndDate")==1))

LagLeadLoc=LagLeadLoc\
    .groupBy("device_id","LocSegment_index_merge")\
        .agg(fn.sum(col("seg_loc_lag")*col("isMinStartDate")).cast("int").alias("loc_closestObs_before"),
             fn.sum(col("seg_loc_lead")*col("isMaxEndDate")).cast("int").alias("loc_closestObs_after"))\
            .withColumn("loc_closestObs_before",fn.when(col("loc_closestObs_before")==0,None).otherwise(col("loc_closestObs_before")))\
                .withColumn("loc_closestObs_after",fn.when(col("loc_closestObs_after")==0,None).otherwise(col("loc_closestObs_after")))

# Join to loc_seg_merge
loc_seg_merge=loc_seg_merge\
    .join(LagLeadLoc,on=["device_id","LocSegment_index_merge"],how="leftouter")


##__ Calculate the upper bound duration of remaining merged segments and remove those smaller than the minimum duration for qualifying as migration (T_meso_min)
# Adjust date_closestObs_before and date_closestObs_after to Null for first and last segments of a user (no observation before and after respectively)
loc_seg_merge=loc_seg_merge\
    .withColumn("date_closestObs_before",fn.when(col("date_closestObs_before")>col("start_date_merge"),None).otherwise(col("date_closestObs_before")))\
        .withColumn("date_closestObs_after",fn.when(col("date_closestObs_after")<col("end_date_merge"),None).otherwise(col("date_closestObs_after")))

# Upper bound duration: gap between lead segment start date and lag segment end date
loc_seg_merge=loc_seg_merge\
    .withColumn("maxDuration",fn.datediff(col("date_closestObs_after"),col("date_closestObs_before"))-1)

# Remove merged segment with maxDuration lower than T_meso_min (keep segments with null max duration)
loc_seg_merge=loc_seg_merge\
    .filter((col("maxDuration").isNull()) | (col("maxDuration")>=T_meso_min))


##__ Resolve overlapping segments
## Note: migration segments within migration segments are not removed.

## Identify segments strictly within another segment
# Calculate the cumulated maximum segment end date
w_cummax=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("LocSegment_index_merge"))\
           .rowsBetween(-sys.maxsize, 0))

loc_seg_merge=loc_seg_merge\
    .withColumn("end_date_merge_cummax",fn.max(col("end_date_merge")).over(w_cummax))

# Dummy equal to 1 if the segment is within another segment
loc_seg_merge=loc_seg_merge\
    .orderBy("device_id","LocSegment_index_merge")\
        .withColumn("within_dummy",fn.when(col("end_date_merge")<col("end_date_merge_cummax"),1).otherwise(0))\
            .drop("end_date_merge_cummax")


## Split the dataset in 2: segments that are within another valid segment and other segments
loc_seg_merge_within=loc_seg_merge\
    .filter(col("within_dummy")==1)\
        .drop("within_dummy")

loc_seg_merge=loc_seg_merge\
    .filter(col("within_dummy")==0)\
        .drop("within_dummy")


## Identify segments that overlap with their preceding segment
# Calculate the end date of the preceding segment (lag end date)
w_lag=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("LocSegment_index_merge")))

loc_seg_merge=loc_seg_merge\
    .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))

loc_seg_merge_within=loc_seg_merge_within\
    .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))

# Calculate a dummy equal to 1 if a segment overlaps with the preceding segment (strictly, i.e. not embedded)
loc_seg_merge=loc_seg_merge\
    .withColumn("overlapPreceding_dummy",fn.when(col("end_date_merge_lag")>col("start_date_merge"),1).otherwise(0))

loc_seg_merge_within=loc_seg_merge_within\
    .withColumn("overlapPreceding_dummy",fn.when(col("end_date_merge_lag")>col("start_date_merge"),1).otherwise(0))

# Indicator equal to 1 if loc_seg_merge (resp. loc_seg_merge_within) has overlapping segments
hasOverlap=loc_seg_merge\
    .select(fn.max("overlapPreceding_dummy").alias("hasOverlap"))\
        .first()['hasOverlap']

hasOverlap_within=loc_seg_merge_within\
    .select(fn.max("overlapPreceding_dummy").alias("hasOverlap"))\
        .first()['hasOverlap']
        
        
## Iterative process to resolve overlap:
    # (i) on both datasets separately, resolve overlap by taking the middle of overlap as cutoff between two consecutive overlapping segments
    # (ii) Re-bind both datasets and re-order by segment start date
    # (iii) Re-identify segments within another segment and re-separate dataset
    # (iv) Repeat the process until there is no overlapping segments left

while (hasOverlap==1 or hasOverlap_within==1):
    ## Resolve overlap in main dataset
    if hasOverlap==1:
        # Calculate the overlap width in days (negative or 0 when there is no overlap)
        loc_seg_merge=loc_seg_merge\
            .withColumn("overlapPreceding_width",fn.datediff(col("end_date_merge_lag"),col("start_date_merge"))+1)

        # Calculate the floor of half the overlap when the overlap is strictly positive (0 otherwise)
        loc_seg_merge=loc_seg_merge\
            .withColumn("halfOverlap",(fn.floor(col("overlapPreceding_width")/2)).cast("int"))
            
        # For overlapping segments, subtract half the overlap to the end date of the 1st segment and add the rest of the overlap to the start date of the second segment
        loc_seg_merge=loc_seg_merge\
            .withColumn("end_date_merge_lag_new",fn.expr("date_add(end_date_merge_lag,-halfOverlap*overlapPreceding_dummy)"))\
                .withColumn("start_date_merge_new",fn.expr("date_add(start_date_merge,(overlapPreceding_width-halfOverlap)*overlapPreceding_dummy)"))\
                    .drop("end_date_merge_lag")

        # Update segment parameters: start_date_merge, end_date_merge, date_closestObs_before, date_closestObs_after, loc_closestObs_before, loc_closestObs_after, maxDuration
        loc_seg_merge=loc_seg_merge\
            .withColumn("end_date_merge",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.lead("end_date_merge_lag_new").over(w_lag)).otherwise(col("end_date_merge")))\
                .withColumn("start_date_merge",fn.when(col("overlapPreceding_dummy")==1,col("start_date_merge_new")).otherwise(col("start_date_merge")))\
                    .drop("overlapPreceding_width","halfOverlap","end_date_merge_lag_new","start_date_merge_new")

        loc_seg_merge=loc_seg_merge\
            .withColumn("date_closestObs_before",fn.when(col("overlapPreceding_dummy")==1,fn.date_add(col("start_date_merge"),-1)).otherwise(col("date_closestObs_before")))\
                .withColumn("date_closestObs_after",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.date_add(col("end_date_merge"),1)).otherwise(col("date_closestObs_after")))
        
        loc_seg_merge=loc_seg_merge\
            .withColumn("loc_closestObs_before",fn.when(col("overlapPreceding_dummy")==1,fn.lag("merged_seg_loc").over(w_lag)).otherwise(col("loc_closestObs_before")))\
                .withColumn("loc_closestObs_after",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.lead("merged_seg_loc").over(w_lag)).otherwise(col("loc_closestObs_after")))\
                    .drop("overlapPreceding_dummy")

    else:
        loc_seg_merge=loc_seg_merge\
            .drop("end_date_merge_lag","overlapPreceding_dummy")
        
    ## Resolve overlap in dataset of segments within segments
    if hasOverlap_within==1:
        # Calculate the overlap width in days (negative or 0 when there is no overlap)
        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("overlapPreceding_width",fn.datediff(col("end_date_merge_lag"),col("start_date_merge"))+1)

        # Calculate the floor of half the overlap when the overlap is strictly positive (0 otherwise)
        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("halfOverlap",(fn.floor(col("overlapPreceding_width")/2)).cast("int"))

        # For overlapping segments, subtract half the overlap to the end date of the 1st segment and add the rest of the overlap to the start date of the second segment
        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("end_date_merge_lag_new",fn.expr("date_add(end_date_merge_lag,-halfOverlap*overlapPreceding_dummy)"))\
                .withColumn("start_date_merge_new",fn.expr("date_add(start_date_merge,(overlapPreceding_width-halfOverlap)*overlapPreceding_dummy)"))\
                    .drop("end_date_merge_lag")
                
        # Update segment parameters: start_date_merge, end_date_merge, date_closestObs_before, date_closestObs_after, maxDuration
        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("end_date_merge",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.lead("end_date_merge_lag_new").over(w_lag)).otherwise(col("end_date_merge")))\
                .withColumn("start_date_merge",fn.when(col("overlapPreceding_dummy")==1,col("start_date_merge_new")).otherwise(col("start_date_merge")))\
                    .drop("overlapPreceding_width","halfOverlap","end_date_merge_lag_new","start_date_merge_new")

        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("date_closestObs_before",fn.when(col("overlapPreceding_dummy")==1,fn.date_add(col("start_date_merge"),-1)).otherwise(col("date_closestObs_before")))\
                .withColumn("date_closestObs_after",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.date_add(col("end_date_merge"),1)).otherwise(col("date_closestObs_after")))
        
        loc_seg_merge_within=loc_seg_merge_within\
            .withColumn("loc_closestObs_before",fn.when(col("overlapPreceding_dummy")==1,fn.lag("merged_seg_loc").over(w_lag)).otherwise(col("loc_closestObs_before")))\
                .withColumn("loc_closestObs_after",fn.when(fn.lead("overlapPreceding_dummy").over(w_lag)==1,fn.lead("merged_seg_loc").over(w_lag)).otherwise(col("loc_closestObs_after")))\
                    .drop("overlapPreceding_dummy")

    else:
        loc_seg_merge_within=loc_seg_merge_within\
            .drop("end_date_merge_lag","overlapPreceding_dummy")
    
    
    ## Rebind all segments together and re-order by start date
    loc_seg_merge=loc_seg_merge\
        .union(loc_seg_merge_within)\
            .orderBy("device_id","start_date_merge")
        
    
    ## Identify segments strictly within another segment
    # Calculate the cumulated maximum segment end date
    loc_seg_merge=loc_seg_merge\
        .withColumn("end_date_merge_cummax",fn.max(col("end_date_merge")).over(w_cummax))

    # Dummy equal to 1 if the segment is within another segment
    loc_seg_merge=loc_seg_merge\
        .orderBy("device_id","LocSegment_index_merge")\
            .withColumn("within_dummy",fn.when(col("end_date_merge")<col("end_date_merge_cummax"),1).otherwise(0))\
                .drop("end_date_merge_cummax")

    
    ## Split the dataset in 2: segments that are within another valid segment and other segments
    loc_seg_merge_within=loc_seg_merge\
        .filter(col("within_dummy")==1)\
            .drop("within_dummy")

    loc_seg_merge=loc_seg_merge\
        .filter(col("within_dummy")==0)\
            .drop("within_dummy")
    
    
    ## Identify segments that overlap with their preceding segment
    # Calculate the end date of the preceding segment (lag end date)
    loc_seg_merge=loc_seg_merge\
        .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))

    loc_seg_merge_within=loc_seg_merge_within\
        .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))

    # Calculate a dummy equal to 1 if a segment overlaps with the preceding segment (strictly, i.e. not embedded)
    loc_seg_merge=loc_seg_merge\
        .withColumn("overlapPreceding_dummy",fn.when(col("end_date_merge_lag")>col("start_date_merge"),1).otherwise(0))

    loc_seg_merge_within=loc_seg_merge_within\
        .withColumn("overlapPreceding_dummy",fn.when(col("end_date_merge_lag")>col("start_date_merge"),1).otherwise(0))

    # Indicator equal to 1 if loc_seg_merge (resp. loc_seg_merge_within) has overlapping segments
    del hasOverlap,hasOverlap_within
    
    hasOverlap=loc_seg_merge\
        .select(fn.max("overlapPreceding_dummy").alias("hasOverlap"))\
            .first()['hasOverlap']

    hasOverlap_within=loc_seg_merge_within\
        .select(fn.max("overlapPreceding_dummy").alias("hasOverlap"))\
            .first()['hasOverlap']


## Now overlap has been resolved, re-bind both datasets and update segment parameters (minDuration, maxDuration)
# Bind
loc_seg_merge=loc_seg_merge\
    .union(loc_seg_merge_within)\
        .drop("end_date_merge_lag","overlapPreceding_dummy")\
            .orderBy("device_id","start_date_merge")

# Update min and max segment duration (lower and upper bound)
loc_seg_merge=loc_seg_merge\
    .withColumn("maxDuration",fn.datediff(col("date_closestObs_after"),col("date_closestObs_before"))-1)\
        .withColumn("minDuration",fn.datediff(col("end_date_merge"),col("start_date_merge"))+1)


## Keep non-home segments longer than T_meso_min
loc_seg_merge=loc_seg_merge\
    .filter((col("merged_seg_loc")!=col("resid_loc_seg")) & ((col("maxDuration").isNull()) | (col("maxDuration")>=T_meso_min)))


## Rename columns and export
loc_seg_merge=loc_seg_merge\
    .select("device_id",
            col("LocSegment_index_merge").alias("segment_index"),
            col("merged_seg_loc").alias("segment_location"),
            col("start_date_merge").alias("start_date"),
            col("end_date_merge").alias("end_date"),
            "resid_loc_seg",
            "minDuration",
            "maxDuration",
            "date_closestObs_before",
            "date_closestObs_after",
            "loc_closestObs_before",
            "loc_closestObs_after"
            )

loc_seg_merge.write.mode("overwrite").parquet(out_dir+"UserSegment_data"+("_"+filter_name if filter_dummy==1 else ""))

shutil.rmtree(out_dir+"temp/loc_seg_merge")