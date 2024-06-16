# Created on Mon May 20 18:48:57 2024

# @author: Paul Blanchard

##__ Description

# This script provides observation gaps for all users. Each observation gap is characterized by a start date and an end date.
# Observation gaps are subsequently used in other scripts for the construction of time-disaggregated migration statistics.


###
#0# Preliminary
###


##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ObsGapDetect')\
    .config("spark.driver.memory", "7g")\
    .config("spark.executor.memory", "7g")\
    .config("spark.driver.maxResultSize", "5g")\
    .getOrCreate()


##__ Load packages
import pyspark.sql.functions as fn
from pyspark.sql.functions import col
from pyspark.sql import Window
import sys


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
#3# Import residence locations datasets
###


##__ Import home locations for users with a unique residence
residLoc_unique=spark.read.parquet(out_dir+"UserUniqueResidence_data"+("_"+filter_name if filter_dummy==1 else ""))


##__ Import home locations by month for users with multiple residence
residLoc_multiple=spark.read.parquet(out_dir+"UserMultipleResidence_data"+("_"+filter_name if filter_dummy==1 else ""))



###
#4# Segment detection
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

# Calculate a location segment index
w_cumsum=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("date").cast("timestamp").cast("long"))\
           .rowsBetween(-sys.maxsize, 0))

daily_loc=daily_loc\
    .withColumn("LocSegment_index",fn.sum("LocSegment_dummy").over(w_cumsum))\
        .withColumn("LocSegment_index",col("LocSegment_index")+1)

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

resid_loc_seg_multi = daily_loc_multi\
    .select("device_id", "date", "LocSegment_index", "resid_loc_multiple")\
    .groupBy("device_id", "LocSegment_index", "resid_loc_multiple")\
    .agg(fn.count(fn.lit(1)).alias("N_daysAtResid"), fn.min("date").alias("min_date"))\
    .withColumn("N_daysAtResid_max", fn.max("N_daysAtResid").over(w_resid_multi))\
    .where((col("N_daysAtResid") == col("N_daysAtResid_max")))\
    .withColumn("LatestResid_date", fn.max("min_date").over(w_resid_multi))\
    .where((col("min_date") == col("LatestResid_date")))\
    .select("device_id", "LocSegment_index", col("resid_loc_multiple").alias("resid_loc_seg"))

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


## Aggregate by user and location segment and for each unit calculate: start date, end date, segment location, home location 
loc_seg=daily_loc\
    .groupBy("device_id","LocSegment_index")\
        .agg(fn.min("date").alias("start_date"),
             fn.max("date").alias("end_date"),
             fn.first("daily_loc").alias("seg_loc"),
             fn.last("resid_loc_seg").alias("resid_loc_seg"))


##__ Merge segments at the same location if they are less than eps_gap_meso days appart
## Such segments may be consecutive -- in which case the user is simply
## unobserved between merged segments -- or non-consecutive -- in which case 
## the user is observed at different locations where she stayed less than eps_gap_meso days.
## This can lead to overlap between merged segments, which we ignore for the purpose of determining observation status.

# Partitition by user and location and calculate lag end date (i.e. end date of previous segment AT THE SAME LOCATION)
w_user_loc=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"))\
       .orderBy(col("LocSegment_index")))

loc_seg=loc_seg\
    .withColumn("end_date_SameLocLag",fn.lag("end_date").over(w_user_loc))

# Calculate the time elapsed since the end of the previous segment at the same location
loc_seg=loc_seg\
    .withColumn("TimeElapsed_segLag",fn.datediff(col("start_date"),col("end_date_SameLocLag"))-1)\
        .drop("end_date_SameLocLag")

# Calculate a dummy equal to 1 when the time elapsed since the end of the previous segment at the same location exceeded eps_gap_meso days, i.e. the segment is not merged with the previous segment at the same location.
loc_seg=loc_seg\
    .withColumn("eps_exceed_dummy",fn.when(col("TimeElapsed_segLag")>eps_gap_meso,1).otherwise(0))\
        .drop("TimeElapsed_segLag")

# Calculate a sub-index that is constant within user-location-segment
w_cumsum2=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"))\
       .orderBy(col("LocSegment_index"))\
           .rowsBetween(-sys.maxsize, 0))

loc_seg=loc_seg\
    .withColumn("LocSegment_subIndex",fn.sum("eps_exceed_dummy").over(w_cumsum2))\
        .drop("eps_exceed_dummy")

# Calculate a new index that merges segments
w_merge=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg"),col("seg_loc"),col("LocSegment_subIndex"))\
       .orderBy(col("LocSegment_index")))

loc_seg=loc_seg\
    .withColumn("LocSegment_index_merge",fn.min(col("LocSegment_index")).over(w_merge))\
        .drop("LocSegment_subIndex")
    
# Calculate the updated start date and end date of merged segments
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


## Calculate the length of merged segments and the number of days at the segment location
# Calculate the length of each segment (in days)
loc_seg=loc_seg\
    .withColumn("segLength",fn.datediff(col("end_date"),col("start_date"))+1)

# Calculate the length of merged segments (equivalent to lower bound duration)
loc_seg=loc_seg\
    .withColumn("mergedSegLength",fn.datediff(col("end_date_merge"),col("start_date_merge"))+1)

# Calculate the total number of days at the merged segment location within the merged segment
loc_seg=loc_seg\
    .withColumn("N_daysAtLoc",fn.sum("segLength").over(w_user_seg))


# In migration detection, only segments with a proportion of days of at least Phi are kept. Here we keep all segments.
#loc_seg=loc_seg\
#    .filter(col("prop_daysAtLoc")>=Phi)


##__ Aggregate at the level of merged groups
resid_BySegMerge = loc_seg\
    .groupBy("device_id", "LocSegment_index_merge", "resid_loc_seg")\
    .agg(fn.sum("segLength").alias("N_daysAtResid"))\
    .withColumn("N_daysAtResid_max", fn.max("N_daysAtResid").over(w_user_seg))\
    .where((col("N_daysAtResid") == col("N_daysAtResid_max")))\
    .withColumn("resid_loc_seg_max", fn.max("resid_loc_seg").over(w_user_seg))\
    .dropDuplicates(["device_id", "LocSegment_index_merge"])\
    .drop("N_daysAtResid", "N_daysAtResid_max", "resid_loc_seg")\
    .withColumnRenamed("resid_loc_seg_max", "resid_loc_seg")

loc_seg_merge = loc_seg\
    .groupBy("device_id", "resid_loc_seg", "LocSegment_index_merge")\
    .agg(fn.first("seg_loc").alias("merged_seg_loc"),
         fn.first("start_date_merge").alias("start_date_merge"),
         fn.first("end_date_merge").alias("end_date_merge"),
         fn.first("mergedSegLength").alias("mergedSegLength"),
         fn.min("end_date_lag").alias("date_closestObs_before"),
         fn.max("start_date_lead").alias("date_closestObs_after"),
         fn.first("N_daysAtLoc").alias("N_daysAtLoc"))
  
loc_seg_merge=loc_seg_merge\
    .join(resid_BySegMerge,on=["device_id","resid_loc_seg","LocSegment_index_merge"],how="left")
        
        
##__ Calculate the upper bound duration of remaining merged segments
# Adjust date_closestObs_before and date_closestObs_after to Null for first and last segments of a user (no observation before and after respectively)
loc_seg_merge=loc_seg_merge\
    .withColumn("date_closestObs_before",fn.when(col("date_closestObs_before")>col("start_date_merge"),None).otherwise(col("date_closestObs_before")))\
        .withColumn("date_closestObs_after",fn.when(col("date_closestObs_after")<col("end_date_merge"),None).otherwise(col("date_closestObs_after")))

# Upper bound duration: gap between lead segment start date and lag segment end date
loc_seg_merge=loc_seg_merge\
    .withColumn("maxDuration",fn.datediff(col("date_closestObs_after"),col("date_closestObs_before"))-1)


##__ Identify segments strictly within another segment and remove them
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

# Remove such segments
loc_seg_merge=loc_seg_merge\
    .filter(col("within_dummy")==0)\
        .drop("within_dummy")


##__ Keep first and last segments of unique user-residence pairs, which are necessarily preceded and followed by a (semi-unbounded) gap
# Calculate min and max segment index by user-residence pairs and keep rows corresponding to min and max respectively
w_userRes=(Window()
   .partitionBy(col("device_id"),col("resid_loc_seg")))

gapFirstLast = loc_seg_merge\
    .withColumn("index_FirstSegment", fn.min("LocSegment_index_merge").over(w_userRes))\
    .withColumn("index_LastSegment", fn.max("LocSegment_index_merge").over(w_userRes))\
    .withColumn("FirstSegment_dummy", fn.when((col("LocSegment_index_merge") == col("index_FirstSegment")), 1).otherwise(0))\
    .withColumn("LastSegment_dummy", fn.when((col("LocSegment_index_merge") == col("index_LastSegment")), 1).otherwise(0))\
    .drop("index_FirstSegment", "index_LastSegment")\
    .filter((col("FirstSegment_dummy") == 1) | (col("LastSegment_dummy") == 1))

# First and last segments in separate datasets
gapFirst=gapFirstLast\
    .filter(col("FirstSegment_dummy")==1)

gapLast=gapFirstLast\
    .filter(col("LastSegment_dummy")==1)

del gapFirstLast

# Construct gap rows
gapFirst = gapFirst\
    .withColumn("obsGap_start_date", fn.lit(None))\
    .withColumn("obsGap_end_date", fn.date_sub("start_date_merge", 1))\
    .withColumn("gapDuration", fn.lit(None))\
    .withColumn("PreviousSeg_loc", fn.lit(None))\
    .withColumn("PreviousSeg_resid_loc", fn.lit(None))\
    .withColumn("PreviousSeg_start_date", fn.lit(None))\
    .withColumn("PreviousSeg_end_date", fn.lit(None))\
    .withColumn("PreviousSeg_date_closestObs_before", fn.lit(None))\
    .withColumn("PreviousSeg_N_daysAtLoc", fn.lit(None))\
    .withColumnRenamed("merged_seg_loc", "NextSeg_loc")\
    .withColumnRenamed("resid_loc_seg", "NextSeg_resid_loc")\
    .withColumnRenamed("end_date_merge", "NextSeg_end_date")\
    .withColumnRenamed("start_date_merge", "NextSeg_start_date")\
    .withColumnRenamed("date_closestObs_after", "NextSeg_date_closestObs_after")\
    .withColumnRenamed("N_daysAtLoc", "NextSeg_N_daysAtLoc")\
    .withColumn("origin_loc", col("NextSeg_resid_loc"))\
    .select("device_id", "origin_loc", "obsGap_start_date", "obsGap_end_date", "gapDuration", "PreviousSeg_loc", "PreviousSeg_resid_loc", "PreviousSeg_start_date", "PreviousSeg_end_date", "PreviousSeg_date_closestObs_before", "PreviousSeg_N_daysAtLoc", "NextSeg_loc", "NextSeg_resid_loc", "NextSeg_start_date", "NextSeg_end_date", "NextSeg_date_closestObs_after", "NextSeg_N_daysAtLoc")

gapLast = gapLast\
    .withColumn("obsGap_start_date", fn.date_add("end_date_merge", 1))\
    .withColumn("obsGap_end_date", fn.lit(None))\
    .withColumn("gapDuration", fn.lit(None))\
    .withColumn("NextSeg_loc", fn.lit(None))\
    .withColumn("NextSeg_resid_loc", fn.lit(None))\
    .withColumn("NextSeg_start_date", fn.lit(None))\
    .withColumn("NextSeg_end_date", fn.lit(None))\
    .withColumn("NextSeg_date_closestObs_after", fn.lit(None))\
    .withColumn("NextSeg_N_daysAtLoc", fn.lit(None))\
    .withColumnRenamed("merged_seg_loc", "PreviousSeg_loc")\
    .withColumnRenamed("resid_loc_seg", "PreviousSeg_resid_loc")\
    .withColumnRenamed("start_date_merge", "PreviousSeg_start_date")\
    .withColumnRenamed("end_date_merge", "PreviousSeg_end_date")\
    .withColumnRenamed("date_closestObs_before", "PreviousSeg_date_closestObs_before")\
    .withColumnRenamed("N_daysAtLoc", "PreviousSeg_N_daysAtLoc")\
    .withColumn("origin_loc", col("PreviousSeg_resid_loc"))\
    .select("device_id", "origin_loc", "obsGap_start_date", "obsGap_end_date", "gapDuration", "PreviousSeg_loc", "PreviousSeg_resid_loc", "PreviousSeg_start_date", "PreviousSeg_end_date", "PreviousSeg_date_closestObs_before", "PreviousSeg_N_daysAtLoc", "NextSeg_loc", "NextSeg_resid_loc", "NextSeg_start_date", "NextSeg_end_date", "NextSeg_date_closestObs_after", "NextSeg_N_daysAtLoc")

# Union results
gapFirstLast=gapFirst\
    .union(gapLast)

##__ Resolve overlap in gapFirstLast in the case of multiple residences 
# Create variables to order the dataset at the user-level
w_userRes=(Window()
   .partitionBy(col("device_id"),col("origin_loc")))

gapFirstLast=gapFirstLast\
    .withColumn("minDate_ByUserOrigin",fn.min("obsGap_start_date").over(w_userRes))

# Calculate dummy equal to 1 when an obs. gap is a first gap preceded by a last gap of the same user at his previous residence location, and which ends before the last obs. gap at the previous residence location started
w_user=(Window()
   .partitionBy(col("device_id"))\
       .orderBy("device_id","minDate_ByUserOrigin","obsGap_start_date"))

gapFirstLast=gapFirstLast\
    .withColumn("origin_loc_lag",fn.lag("origin_loc").over(w_user))\
    .withColumn("obsGap_start_date_lag",fn.lag("obsGap_start_date").over(w_user))\
    .withColumn("gapPreceding_dummy",fn.when((col("origin_loc")!=col("origin_loc_lag")) & (fn.datediff(col("obsGap_start_date_lag"),col("obsGap_end_date"))>1),1).otherwise(0))\
    .drop("origin_loc_lag")\
    .withColumn("gapPreceding_width",fn.datediff(col("obsGap_start_date_lag"),col("obsGap_end_date"))-1)\
    .withColumn("halfGap",(fn.floor(col("gapPreceding_width")/2)).cast("int"))\
    .withColumn("obsGap_start_date_lag_new",fn.expr("date_sub(obsGap_start_date_lag,halfGap*gapPreceding_dummy)"))\
    .drop("obsGap_start_date_lag")\
    .withColumn("obsGap_end_date_new",fn.expr("date_add(obsGap_end_date,(gapPreceding_width-halfGap)*gapPreceding_dummy)"))\
    .drop("gapPreceding_dummy","gapPreceding_width","halfGap")\
    .withColumn("obsGap_start_date_new",fn.lead("obsGap_start_date_lag_new").over(w_user))\
    .drop("obsGap_start_date_lag_new")\
    .withColumn("obsGap_start_date",fn.when(col("obsGap_start_date_new").isNotNull(),col("obsGap_start_date_new")).otherwise(col("obsGap_start_date")))\
    .withColumn("obsGap_end_date",fn.when(col("obsGap_end_date_new").isNotNull(),col("obsGap_end_date_new")).otherwise(col("obsGap_end_date")))\
    .drop("obsGap_end_date_new","obsGap_start_date_new","minDate_ByUserOrigin")


##__ Back to segments preceded by a gap in loc_seg_merge dataframe.
## Dummy equal to 1 if the segment is preceded by a gap
# Calculate cumulated lagged maximum end date
w_lag=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("LocSegment_index_merge")))

loc_seg_merge=loc_seg_merge\
    .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))\
        .withColumn("end_date_merge_lag_cummax",fn.max(col("end_date_merge_lag")).over(w_cummax))
                

# Dummy and lead dummy
loc_seg_merge=loc_seg_merge\
    .withColumn("PrecededByGap",fn.when(fn.datediff(col("start_date_merge"),col("end_date_merge_lag_cummax"))>1,1).otherwise(0))\
        .withColumn("PrecededByGap_lead",fn.lead("PrecededByGap").over(w_lag))


## Note: Rows with segments preceded by a gap will be transformed to observation gap rows


## Keep segments preceded or followed by a gap
loc_seg_merge=loc_seg_merge\
    .filter((col("PrecededByGap")==1) | (col("PrecededByGap_lead")==1))\
        .drop("PrecededByGap_lead")


## Parameters of preceding segment
# Start date, end date, segment duration (min), closest date of observation before start date
loc_seg_merge = loc_seg_merge\
    .withColumn("PreviousSeg_start_date", fn.lag("start_date_merge").over(w_lag))\
    .withColumn("PreviousSeg_end_date", col("end_date_merge_lag"))\
    .withColumn("PreviousSeg_minDuration", fn.datediff(col("PreviousSeg_end_date"), col("PreviousSeg_start_date"))+1)\
    .withColumn("PreviousSeg_date_closestObs_before", fn.lag("date_closestObs_before").over(w_lag))\
    .withColumn("PreviousSeg_resid_loc", fn.lag("resid_loc_seg").over(w_lag))

# Segment location, residence location
loc_seg_merge=loc_seg_merge\
    .withColumn("PreviousSeg_loc",fn.lag("merged_seg_loc").over(w_lag))
    
# Maximum duration including observational gap(s)
loc_seg_merge=loc_seg_merge\
    .withColumn("PreviousSeg_maxDuration_withGap",fn.lag("maxDuration").over(w_lag))

# Number of observed days at the segment location
loc_seg_merge=loc_seg_merge\
    .withColumn("PreviousSeg_N_daysAtLoc",fn.lag("N_daysAtLoc").over(w_lag))


## Keep only rows with PrecededByGap dummy equal to 1
loc_seg_merge=loc_seg_merge\
    .filter(col("PrecededByGap")==1)\
        .drop("PrecededByGap")
        

## Parameters of following segment
# Start date, end date, segment duration (min), closest date of observation after end date
loc_seg_merge = loc_seg_merge\
    .withColumnRenamed("start_date_merge", "NextSeg_start_date")\
    .withColumnRenamed("end_date_merge", "NextSeg_end_date")\
    .withColumnRenamed("mergedSegLength", "NextSeg_minDuration")\
    .withColumnRenamed("date_closestObs_after", "NextSeg_date_closestObs_after")

# Segment location, residence location
loc_seg_merge=loc_seg_merge\
    .withColumnRenamed("merged_seg_loc","NextSeg_loc")\
        .withColumnRenamed("resid_loc_seg","NextSeg_resid_loc")
    
# Maximum duration including observational gap
loc_seg_merge=loc_seg_merge\
    .withColumnRenamed("maxDuration","NextSeg_maxDuration_withGap")

# Number of observed days at the segment location
loc_seg_merge=loc_seg_merge\
    .withColumnRenamed("N_daysAtLoc","NextSeg_N_daysAtLoc")


## Start date, end date and duration of observational gaps
loc_seg_merge=loc_seg_merge\
    .withColumn("obsGap_start_date",fn.date_add(col("PreviousSeg_end_date"),1))\
        .withColumn("obsGap_end_date",fn.date_add(col("NextSeg_start_date"),-1))\
            .withColumn("gapDuration",fn.datediff(col("obsGap_end_date"),col("obsGap_start_date"))+1)


## Residence location associated with the gap - by default the residence location that was assigned to the previous segment
loc_seg_merge=loc_seg_merge\
    .withColumn("origin_loc",col("PreviousSeg_resid_loc"))


## Maximum proportion of days at the segment location for preceding and following segments, when including the gap(s)
loc_seg_merge=loc_seg_merge\
    .withColumn("PreviousSeg_maxProp_daysAtLoc",(col("PreviousSeg_N_daysAtLoc")+col("PreviousSeg_maxDuration_withGap")-col("PreviousSeg_minDuration"))/col("PreviousSeg_maxDuration_withGap"))\
        .withColumn("NextSeg_maxProp_daysAtLoc",(col("NextSeg_N_daysAtLoc")+col("NextSeg_maxDuration_withGap")-col("NextSeg_minDuration"))/col("NextSeg_maxDuration_withGap"))
        

##__ Finalize and export
## Union regular observational gaps and gaps corresponding to periods before/after the first/last observation
obsGap_final=loc_seg_merge\
    .select("device_id","origin_loc","obsGap_start_date","obsGap_end_date","gapDuration","PreviousSeg_loc","PreviousSeg_resid_loc","PreviousSeg_start_date","PreviousSeg_end_date","PreviousSeg_date_closestObs_before","PreviousSeg_N_daysAtLoc","NextSeg_loc","NextSeg_resid_loc","NextSeg_start_date","NextSeg_end_date","NextSeg_date_closestObs_after","NextSeg_N_daysAtLoc")\
        .union(gapFirstLast)


## Export
obsGap_final.write.mode("overwrite").parquet(out_dir+"UserObservationGap"+("_"+filter_name if filter_dummy==1 else ""))

