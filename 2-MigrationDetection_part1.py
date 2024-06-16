#Created on Mon May 20 16:02:01 2024

#@author: Paul Blanchard

##__ Description

# MigrationDetection_part1 proceeds with the macro-segment detection procedure.
# The daily location dataset is taken as input and the script outputs two datasets:
    # output 1: residence location by user, for those users without multiple macro-segments detected (usually the vast majority of users)
    # output 2: residence location by user-month, for users with multiple macro-segments detected


###
#0# Preliminary
###

##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationDetection_part1')\
    .config("spark.driver.memory", "5g")\
        .config("spark.executor.memory", "5g")\
            .config("spark.driver.maxResultSize", "5g")\
                .getOrCreate()


##__ Load packages
import pyspark.sql.functions as fn
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql import Window
import sys
import operator
import datetime


##__ Path pointing to the folder where the daily locations dataset was exported (Script MigrationDetection_part1.py)
out_dir=".../MigrationDetection_output/"


##__ Option to use filtered input that keeps a subset of users, for instance to impose constraints on observational characteristics (e.g. length and frequency of observation).
# Filtering dummy: if 1 then a filter is applied.
filter_dummy = 1

# Filter name, used in the name of exported outputs.
filter_name=""


##__ Set migration detection parameters (refer to paper for more details)
# Minimum duration of a macro-segment 
T_meso_max=6 #in months

# Maximum observation gaps allowed
eps_gap_macro=6 #in months


###
#1# Some useful user-defined functions
###


##__ Function that takes a segment's parameters and convert it to a list of lists where each element is [device_id,month,year,resid_loc]
def generate_monthlyRes_seq(start_month,start_year,end_month,end_year,userID,loc):
    ## Start and end dates
    start_date=datetime.date(start_year,start_month,1)
    end_date=datetime.date(end_year,end_month,2)
    
    
    ## All dates from start to end
    allDates=[start_date + datetime.timedelta(days=x) for x in range(0, (end_date-start_date).days)]
    
    
    ## Transform all dates to string with format %Y%m and remove duplicates
    allDates_Ym=list(map(lambda el:el.strftime("%Y%m"), allDates))
    Ym_unique = list(dict.fromkeys(allDates_Ym))
    
    
    ## Create complete sequence of months and sequence of years
    month_seq=list(map(lambda el:[int(el[-2:])], Ym_unique))
    year_seq=list(map(lambda el:[int(el[:4])], Ym_unique))
    
    
    ## device_id sequence, location sequence and final result
    device_id_seq=[userID]*len(month_seq)
    device_id_seq=list(map(lambda el:[el], device_id_seq))
    
    loc_seq=[loc]*len(month_seq)
    loc_seq=list(map(lambda el:[el], loc_seq))
    
    result=list(map(operator.add,device_id_seq,month_seq))
    result=list(map(operator.add,result,year_seq))
    result=list(map(operator.add,result,loc_seq))

    return result

udf_generate_monthlyRes_seq = udf(lambda a,b,c,d,e,f :generate_monthlyRes_seq(a,b,c,d,e,f),ArrayType(ArrayType(StringType())) )

##__ Function that creates a column from a column of lists taking the kth element of each list
def extract_kth(el,k):
    return el[k]

udf_extract_kth = udf(lambda x,n :extract_kth(x,n),StringType() )


###
#2# Import the dataset of daily locations (calculated in MigrationDetection_part0.py)
###


##__ Import
daily_loc=spark.read.parquet(out_dir+"dailyLoc_data"+("_"+filter_name if filter_dummy==1 else ""))


##__ Keep a separate dataframe with min and max date observed by device (used later to adjust lead and lag dates of final segments)
# Re-create date column in daily_loc from year, month and day columns
daily_loc=daily_loc\
    .withColumn("date",fn.to_date(fn.concat_ws("-",col("year"),col("month"),col("day")),format="yyyy-M-d"))

# Table with min and max dates for each user
MinMaxDates=daily_loc\
    .groupBy("device_id")\
        .agg(fn.min("date").alias("minDate"),\
             fn.max("date").alias("maxDate"))


###
#3# Hierarchical method to determine Monthly location
###


## Calculate a monthly location as the modal daily location imposing a minimum of 10 days observed (see MigrationDetection_part0 for detailed comments on this pipeline)
w_month=Window.partitionBy("device_id","year","month")

monthly_loc=daily_loc\
    .select("device_id","daily_loc","year","month")\
        .groupBy("device_id","year","month","daily_loc")\
            .agg(fn.count(fn.lit(1)).alias("N_daysAtLoc"))\
                .withColumn("N_daysAtLoc_max",fn.max("N_daysAtLoc").over(w_month))\
                    .withColumn("N_days_total",fn.sum("N_daysAtLoc").over(w_month))\
                        .where((col("N_daysAtLoc")==col("N_daysAtLoc_max")))\
                            .withColumn("daily_loc_max",fn.max("daily_loc").over(w_month))\
                                .dropDuplicates(["device_id","year","month"])\
                                    .drop("N_daysAtLoc","N_daysAtLoc_max","daily_loc")\
                                        .withColumnRenamed("daily_loc_max","monthly_loc")\
                                            .where(col("N_days_total")>=10)


###
#4# Detection of macro-movements
###


##__ Infer a primary unique residence location for each device, defined as the modal daily location
w_user=Window.partitionBy("device_id")

residLoc0=daily_loc\
    .select("device_id","daily_loc")\
        .groupBy("device_id","daily_loc")\
            .agg(fn.count(fn.lit(1)).alias("N_daysAtLoc"))\
                .withColumn("N_daysAtLoc_max",fn.max("N_daysAtLoc").over(w_user))\
                    .where((col("N_daysAtLoc")==col("N_daysAtLoc_max")))\
                        .withColumn("daily_loc_max",fn.max("daily_loc").over(w_user))\
                            .dropDuplicates(["device_id"])\
                                .drop("N_daysAtLoc","N_daysAtLoc_max","daily_loc")\
                                    .withColumnRenamed("daily_loc_max","resid_loc0")


##__ Re-create a date column in monthly location dataset
# Day column (constant equal to 1)
monthly_loc=monthly_loc\
    .withColumn("day",fn.lit(1))

# Date in yyyy-M-d format
monthly_loc=monthly_loc\
    .withColumn("date",fn.to_date(fn.concat_ws("-",col("year"),col("month"),col("day")),"yyyy-M-d"))


##__ Detect contiguous monthly locations
# Calculate lag monthly location and lag date
del w_user
w_user=(Window()\
        .partitionBy(col("device_id"))\
            .orderBy(col("date").cast("timestamp").cast("long")))

monthly_loc=monthly_loc\
    .withColumn("monthly_loc_lag",fn.lag("monthly_loc").over(w_user))\
        .withColumn("date_lag",fn.lag("date").over(w_user))

# Calculate gap with lag observation in months
monthly_loc=monthly_loc\
    .withColumn("gapLag",fn.round(fn.months_between(col("date"),col("date_lag")),0)-1)

# Calculate a dummy equal to 1 if there is a change in the monthly location OR a gap greater than eps_gap_macro relative to the previous observation, i.e. a new segment starts.
monthly_loc=monthly_loc\
    .withColumn("ResSegment_dummy",fn.when((col("monthly_loc")!=col("monthly_loc_lag")) | (col("gapLag")>eps_gap_macro),1).otherwise(0))

# Calculate a monthly location group index "ResSegment_index"
w_cumsum=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("date").cast("timestamp").cast("long"))\
           .rowsBetween(-sys.maxsize, 0))

monthly_loc=monthly_loc\
    .withColumn("ResSegment_index",fn.sum("ResSegment_dummy").over(w_cumsum))\
        .withColumn("ResSegment_index",col("ResSegment_index")+1)

# Drop columns that will no longer be used
monthly_loc=monthly_loc\
    .drop("monthly_loc_lag","date_lag")

# Calculate the sum of ResSegment_dummy for each user. A strictly positive value indicates a change in monthly location or a gap greater than eps_gap_macro, so we cannot rule out the possibility of a macro-movement at this point.
del w_user
w_user=(Window()\
        .partitionBy(col("device_id")))

monthly_loc=monthly_loc\
    .withColumn("ResSegment_dummy_sum",fn.sum("ResSegment_dummy").over(w_user))


##__ First subset of residence location: users with a single monthly location group index at this point, i.e. no change in monthly location and no gap greater than eps_gap_macro
# Build first subset of residence locations
residLoc1=monthly_loc\
    .where(col("ResSegment_dummy_sum")==0)\
        .groupBy("device_id")\
            .agg(fn.first("monthly_loc").alias("resid_loc_unique"))

# Remove those users from monthly_loc
monthly_loc=monthly_loc\
    .where(col("ResSegment_dummy_sum")>0)


##__ Aggregate by user and monthly location group and for each unit calculate: start date, end date, segment location
resLoc_seg=monthly_loc\
    .groupBy("device_id","ResSegment_index")\
        .agg(fn.min("date").alias("start_date"),fn.max("date").alias("end_date"),fn.first("monthly_loc").alias("resSeg_loc"))


##__ Merge monthly location groups separated by temporary migration spells (i.e. less than T_meso_max). This can create overlap that is dealt with later on.
# Partitition by user and location and calculate lag end date (i.e. end date of previous group AT THE SAME LOCATION)
w_user_loc=(Window()
   .partitionBy(col("device_id"),col("resSeg_loc"))\
       .orderBy(col("ResSegment_index")))

resLoc_seg=resLoc_seg\
    .withColumn("end_date_SameLocLag",fn.lag("end_date").over(w_user_loc))

# Calculate the time elapsed since the end of the previous segment at the same location
resLoc_seg=resLoc_seg\
    .withColumn("TimeElapsed_segLag",fn.round(fn.months_between(col("start_date"),col("end_date_SameLocLag")),0)-1)\
        .drop("end_date_SameLocLag")

# Calculate a dummy equal to 1 when the time elapsed since the end of the previous segment at the same location exceeded T_meso_max months, i.e. the group is not merged with the previous group at the same location.
resLoc_seg=resLoc_seg\
    .withColumn("T_meso_max_exceed_dummy",fn.when(col("TimeElapsed_segLag")>=T_meso_max,1).otherwise(0))\
        .drop("TimeElapsed_segLag")

# Calculate a sub-index that is constant within user-location-group
w_cumsum2=(Window()
   .partitionBy(col("device_id"),col("resSeg_loc"))\
       .orderBy(col("ResSegment_index"))\
           .rowsBetween(-sys.maxsize, 0))

resLoc_seg=resLoc_seg\
    .withColumn("ResSegment_subIndex",fn.sum("T_meso_max_exceed_dummy").over(w_cumsum2))\
        .drop("T_meso_max_exceed_dummy")

# Calculate a new index that merges groups
w_merge=(Window()
   .partitionBy(col("device_id"),col("resSeg_loc"),col("ResSegment_subIndex"))\
       .orderBy(col("ResSegment_index")))

resLoc_seg=resLoc_seg\
    .withColumn("ResSegment_index_merge",fn.min(col("ResSegment_index")).over(w_merge))\
        .drop("ResSegment_subIndex")
    
# Calculate the updated start date and end date of merged segments
w_user_seg=(Window()
   .partitionBy(col("device_id"),col("ResSegment_index_merge")))

resLoc_seg=resLoc_seg\
    .withColumn("start_date_merge",fn.min(col("start_date")).over(w_user_seg))\
        .withColumn("end_date_merge",fn.max(col("end_date")).over(w_user_seg))


##__ Second subset of users with a unique residence: those that have strictly less than 2 segments of at least T_meso_max months
# Calculate the length of each segment
resLoc_seg=resLoc_seg\
    .withColumn("ResSeg_minDuration",fn.round(fn.months_between(col("end_date_merge"),col("start_date_merge")),0)+1)

# Reduce to a dataframe of unique merged segments
resLoc_merge_seg=resLoc_seg\
    .groupBy("device_id","ResSegment_index_merge","resSeg_loc","start_date_merge","end_date_merge")\
        .agg(fn.first("ResSeg_minDuration").alias("ResSeg_minDuration"))

# Dummy equal to 1 for users with at least 2 segments of at least T_meso_max months
del w_user
w_user=(Window()
   .partitionBy(col("device_id")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("dummy_exceed_T_meso_max",fn.when(col("ResSeg_minDuration")>=T_meso_max,1).otherwise(0))\
        .withColumn("N_seg_exceed_T_meso_max",fn.sum("dummy_exceed_T_meso_max").over(w_user))\
            .withColumn("dummy_AtLeast2",fn.when(col("N_seg_exceed_T_meso_max")>1,1).otherwise(0))

# Build second subset of residence locations (initialize with unique device_id )
residLoc2=resLoc_merge_seg\
    .where(col("dummy_AtLeast2")==0)\
        .groupBy("device_id")\
            .agg(fn.first("resSeg_loc").alias("useless_column"))

# Join initial unique residence location estimate
residLoc2=residLoc2\
    .join(residLoc0,on="device_id")\
        .drop("useless_column")\
            .withColumnRenamed("resid_loc0","resid_loc_unique")

# Remove those users from resLoc_merge_seg
resLoc_merge_seg=resLoc_merge_seg\
    .where(col("dummy_AtLeast2")==1)\
        .drop("dummy_exceed_T_meso_max","N_seg_exceed_T_meso_max","dummy_AtLeast2")


###
#5# Resolve overlapping segments
###


##__ Remove segments smaller than T_meso_max: they cannot be residence segments.
resLoc_merge_seg=resLoc_merge_seg\
    .where(col("ResSeg_minDuration")>=T_meso_max)


##__ Identify and remove segments strictly embedded within a bigger segment
# Calculate the end date of preceding segment
w_lag=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("ResSegment_index_merge")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_lag))

# Embedded segment dummy: when segment ends before the end of previous segment
resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("EmbedSeg_dummy",fn.when((col("end_date_merge")<col("end_date_merge_lag")),1).otherwise(0))

# Remove embedded segments
resLoc_merge_seg=resLoc_merge_seg\
    .filter(col("EmbedSeg_dummy")!=1)\
        .drop("EmbedSeg_dummy")


##__ Deal with cases where two consecutive segments overlap:
## (i) assign the overlap to the longest (in case of equality: assign to the first one by default)
## (ii) recalculate start and end dates of segments
## (iii) iterate until no overlapping segments left (there can be cases of multiple overlaps)

# Calculate the start date of the following segment
w_lead=(Window()
   .partitionBy(col("device_id"))\
       .orderBy(col("ResSegment_index_merge")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("start_date_merge_lead",fn.lead("start_date_merge").over(w_lead))

# Dummy equal to 1 if a segment overlaps preceding segment and dummy equal to 1 if a segment overlaps following segment
resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("overlap_prev_dummy",fn.when((col("start_date_merge")<=col("end_date_merge_lag")),1).otherwise(0))\
        .withColumn("overlap_follow_dummy",fn.when((col("end_date_merge")>=col("start_date_merge_lead")),1).otherwise(0))

# Check if there is any overlap and iterate procedure until there is none left
check_overlap=resLoc_merge_seg.groupBy().sum("overlap_prev_dummy").collect()[0][0]

if check_overlap is not None:
    while check_overlap>0:
        # Duration of preceding and following segment
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("ResSeg_minDuration_lag",fn.lag("ResSeg_minDuration").over(w_lag))\
                .withColumn("ResSeg_minDuration_lead",fn.lead("ResSeg_minDuration").over(w_lead))

        # Dummy equal to 1 if preceding segment is longer (resp. following segment is longer)
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("PreviousLonger_dummy",fn.when((col("ResSeg_minDuration_lag")>=col("ResSeg_minDuration")),1).otherwise(0))\
                .withColumn("FollowLonger_dummy",fn.when((col("ResSeg_minDuration_lead")>col("ResSeg_minDuration")),1).otherwise(0))\
                    .drop("ResSeg_minDuration_lag","ResSeg_minDuration_lead")

        # If there is overlap with preceding segment and preceding segment is longer --> keep preceding segment intact and assign new start date to current segment as the end date of preceding segment + 1 month
        # If there is overlap with following segment and following segment is strictly longer --> keep following segment intact and assign new end date to current segment as the start date of following segment - 1 month
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("start_date_merge_prev",fn.when((col("overlap_prev_dummy")==1) & (col("PreviousLonger_dummy")==1),fn.add_months(col("end_date_merge_lag"),1)).otherwise(col("start_date_merge")))\
                .withColumn("end_date_merge_follow",fn.when((col("overlap_follow_dummy")==1) & (col("FollowLonger_dummy")==1),fn.add_months(col("start_date_merge_lead"),-1)).otherwise(col("end_date_merge")))\
                    .drop("overlap_prev_dummy","overlap_follow_dummy")

        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("start_date_merge_final",fn.greatest(col("start_date_merge_prev"),col("start_date_merge")))\
                .withColumn("end_date_merge_final",fn.least(col("end_date_merge_follow"),col("end_date_merge")))\
                    .drop("start_date_merge","end_date_merge","start_date_merge_prev","end_date_merge_follow")\
                        .withColumnRenamed("start_date_merge_final","start_date_merge")\
                            .withColumnRenamed("end_date_merge_final","end_date_merge")

        # Remove segments that ended up with a start date greater than the end date
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("remove_segment",fn.when(col("start_date_merge")>col("end_date_merge"),1).otherwise(0))

        resLoc_merge_seg=resLoc_merge_seg\
            .where(col("remove_segment")==0)\
                .drop("remove_segment")

        # Check for embedded segments and remove
        w_LagOrLead=(Window()
           .partitionBy(col("device_id"))\
               .orderBy(col("start_date_merge").cast("timestamp").cast("long")))

        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("start_date_merge_lag",fn.lag("start_date_merge").over(w_LagOrLead))\
                .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_LagOrLead))\
                    .withColumn("start_date_merge_lead",fn.lead("start_date_merge").over(w_LagOrLead))\
                        .withColumn("end_date_merge_lead",fn.lead("end_date_merge").over(w_LagOrLead))
                        
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("EmbedSeg_dummy",fn.when(((col("start_date_merge")>=col("start_date_merge_lag")) & (col("end_date_merge")<=col("end_date_merge_lag"))) | ((col("start_date_merge")>=col("start_date_merge_lead")) & (col("end_date_merge")<=col("end_date_merge_lead"))),1).otherwise(0))

        resLoc_merge_seg=resLoc_merge_seg\
            .filter(col("EmbedSeg_dummy")!=1)\
                .drop("EmbedSeg_dummy")

        # Remove segments smaller than T_meso_max
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("ResSeg_minDuration",fn.round(fn.months_between(col("end_date_merge"),col("start_date_merge")),0)+1)

        resLoc_merge_seg=resLoc_merge_seg\
            .where(col("ResSeg_minDuration")>=T_meso_max)
            
        # Calculate the new start date of the following segment and end date of previous segment
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("start_date_merge_lead",fn.lead("start_date_merge").over(w_LagOrLead))\
                .withColumn("end_date_merge_lag",fn.lag("end_date_merge").over(w_LagOrLead))

        # Overlap dummies
        resLoc_merge_seg=resLoc_merge_seg\
            .withColumn("overlap_prev_dummy",fn.when((col("start_date_merge")<=col("end_date_merge_lag")),1).otherwise(0))\
                .withColumn("overlap_follow_dummy",fn.when((col("end_date_merge")>=col("start_date_merge_lead")),1).otherwise(0))

        # Update check variable used as argument in while loop
        check_overlap=resLoc_merge_seg.groupBy().sum("overlap_prev_dummy").collect()[0][0]



###
#6# Third subset of users for whom there are no more than 2 segments>T_meso_max months after resolving overlaps
###


##__ Calculate the length of each segment
resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("ResSeg_minDuration",fn.round(fn.months_between(col("end_date_merge"),col("start_date_merge")),0)+1)

##__ Dummy equal to 1 for users with at least 2 segments of at least T_meso_max months
del w_user
w_user=(Window()
   .partitionBy(col("device_id")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("dummy_exceed_T_meso_max",fn.when(col("ResSeg_minDuration")>=T_meso_max,1).otherwise(0))\
        .withColumn("N_seg_exceed_T_meso_max",fn.sum("dummy_exceed_T_meso_max").over(w_user))\
            .withColumn("dummy_AtLeast2",fn.when(col("N_seg_exceed_T_meso_max")>1,1).otherwise(0))

##__ Build the third subset of devices with a uniqueresidence locations (initialize with unique device_id )
residLoc3=resLoc_merge_seg\
    .where(col("dummy_AtLeast2")==0)\
        .groupBy("device_id")\
            .agg(fn.first("resSeg_loc").alias("useless_column"))

##__ Join initial unique residence location estimate
residLoc3=residLoc3\
    .join(residLoc0,on="device_id")\
        .drop("useless_column")\
            .withColumnRenamed("resid_loc0","resid_loc_unique")

##__ Remove those users from resLoc_seg_merge
resLoc_merge_seg=resLoc_merge_seg\
    .where(col("dummy_AtLeast2")==1)\
        .drop("dummy_exceed_T_meso_max","N_seg_exceed_T_meso_max","dummy_AtLeast2")


###
#7# Calculate a monthly residence location for users in the last subset, i.e. those with multiple macro-locations detected
###
        

##__ Adjust start and end dates of remaining segments
# Join min/max observation dates
resLoc_merge_seg=resLoc_merge_seg\
    .join(MinMaxDates,on="device_id",how="leftouter")

# Adjust start (resp. end) date of the first (resp. last) segment to the min (resp. max) date of observation.
del w_user
w_user=(Window()\
        .partitionBy(col("device_id")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("ResSegment_index_merge_min",fn.min("start_date_merge").over(w_user))\
        .withColumn("ResSegment_index_merge_max",fn.max("start_date_merge").over(w_user))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("start_date_merge",fn.when(col("start_date_merge")==col("ResSegment_index_merge_min"),col("minDate")).otherwise(col("start_date_merge")))\
        .withColumn("end_date_merge",fn.when(col("start_date_merge")==col("ResSegment_index_merge_max"),col("maxDate")).otherwise(col("end_date_merge")))

# Leave start dates as they are and adjust end dates as the day before the subsequent segment starts.
w_lead=(Window()\
    .partitionBy(col("device_id"))\
       .orderBy(col("start_date_merge").cast("timestamp").cast("long")))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("start_date_merge_lead",fn.lead("start_date_merge").over(w_lead))

resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("end_date_merge",fn.when(col("start_date_merge")!=col("ResSegment_index_merge_max"),fn.date_add(col("start_date_merge_lead"),-1)).otherwise(col("end_date_merge")))

resLoc_merge_seg=resLoc_merge_seg\
    .select("device_id","start_date_merge","end_date_merge",col("resSeg_loc").alias("resid_loc_multiple"))


##__ Build a monthly residence location DataFrame for the last subset
# Create start and and end month and year columns
resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("start_year",fn.year("start_date_merge"))\
        .withColumn("start_month",fn.month("start_date_merge"))\
            .withColumn("end_year",fn.year("end_date_merge"))\
                .withColumn("end_month",fn.month("end_date_merge"))

# Create a column of lists of lists
resLoc_merge_seg=resLoc_merge_seg\
    .withColumn("list_seq",udf_generate_monthlyRes_seq(col("start_month"),col("start_year"),col("end_month"),col("end_year"),col("device_id"),col("resid_loc_multiple").cast(IntegerType())))

# Explode created column in a new Dataframe and extract values in distinct columns
residLoc_multiple=resLoc_merge_seg\
    .select(fn.explode(col("list_seq")).alias("list_seq"))

residLoc_multiple=residLoc_multiple\
    .withColumn("device_id",udf_extract_kth(col("list_seq"),fn.lit(0)))\
        .withColumn("month",udf_extract_kth(col("list_seq"),fn.lit(1)))\
            .withColumn("year",udf_extract_kth(col("list_seq"),fn.lit(2)))\
                .withColumn("resid_loc_multiple",udf_extract_kth(col("list_seq"),fn.lit(3)))

residLoc_multiple=residLoc_multiple\
    .select("device_id",col("month").cast(IntegerType()),col("year").cast(IntegerType()),col("resid_loc_multiple").cast(IntegerType()))


###
#8# Create a subset of residence location for users lost in the final process for multiple residence locations
###


## Merge residLoc1, residLoc2, residLoc3 row-wise: dataset of residence location for users for which it is unique
residLoc_unique=residLoc1.union(residLoc2)
residLoc_unique=residLoc_unique.union(residLoc3)


## Create a dataset of unique users for whom a residence (or multiple residence) was found
residLoc_found=residLoc_unique\
    .select("device_id",col("resid_loc_unique").alias("joined_column"))\
        .union(residLoc_multiple\
               .dropDuplicates(["device_id"])\
                   .select("device_id",col("resid_loc_multiple").alias("joined_column")))


## Join to residLoc0 and find null values to select the last subset of users with a unique residence location
residLoc4=residLoc0\
    .join(residLoc_found,on="device_id",how="leftouter")

residLoc4=residLoc4\
    .filter(fn.isnull("joined_column"))\
        .drop("joined_column")\
            .withColumnRenamed("resid_loc0","resid_loc_unique")

residLoc_unique=residLoc_unique.union(residLoc4)


###
#9# Export
###


##__ Export the residence location data separately for users with unique and multiple residence locations over the period of observation
residLoc_unique.write.mode("overwrite").parquet(out_dir+"UserUniqueResidence_data_"+("_"+filter_name if filter_dummy==1 else ""))
residLoc_multiple.write.mode("overwrite").parquet(out_dir+"UserMultipleResidence_data_"+("_"+filter_name if filter_dummy==1 else ""))
