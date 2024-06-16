# Created on Mon May 15 18:14:54 2023

# @author: Paul Blanchard

##__ Description

# This script imports user-level meso-segments and calculates migration flows (departures and returns) by origin-destination and specified time unit


###
#0# Preliminary
###


##__ Create spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('MigrationStatistics_part2')\
    .config("spark.driver.memory", "10g")\
        .config("spark.executor.memory", "10g")\
            .config("spark.driver.maxResultSize", "8g")\
                .getOrCreate()


##__ Load packages
from pyspark.sql.functions import col
import numpy as np
from pyspark.sql.types import IntegerType, StringType, DateType
import pyspark.sql.functions as fn


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
# Minimum duration for a meso-segment to be classified as a temporary migration event.
# Possibility to specify multiple values, in which case migration flows for these distinct values will be calculated and exported in separate files.
T_meso_min=[20,30,60] #in days

# Maximum observation gap allowed for consecutive observations at a single location to be grouped in the same meso-segment
eps_gap_meso=7 #in days

# Minimum fraction of days at location within a segment to be valid
Phi=0.5


##__ Specify tolerance parameter
eps_tol=7 #in days


###
#1# Functions to generate sequences of time units
###


##__ Full sequences of time units from 2010 to 2026
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
#2# Import user-level meso-segments DataFrame
###

## Note: in this version, UserSegment dataset contains only non-home segments with a max duration of at least 20 days.
## The constraint on the proportion of days observed at the location within a segment (Phi) is already incorporated in the migration detection algorithm (script 3).

UserSegment=spark.read.parquet(out_dir+"UserSegment_data"+("_"+filter_name if filter_dummy==1 else ""))


###
#3# Calculate useful variable in UserSegment (mostly date-derived)
###


# Create week of the year, month of the year, and year columns for start and end dates of each segment
# Create week and month indices from start and end segment dates
UserSegment = UserSegment\
    .withColumn("start_weekOfYear", fn.weekofyear("start_date"))\
        .withColumn("start_month", fn.month("start_date"))\
            .withColumn("start_year", fn.year("start_date"))\
                .withColumn("end_weekOfYear", fn.weekofyear("end_date"))\
                    .withColumn("end_month", fn.month("end_date"))\
                        .withColumn("end_year", fn.year("end_date"))

# Same thing for closest dates before and after segments
UserSegment = UserSegment\
    .withColumn("date_closestObs_before_weekOfYear", fn.weekofyear("date_closestObs_before"))\
        .withColumn("date_closestObs_before_month", fn.month("date_closestObs_before"))\
            .withColumn("date_closestObs_before_year", fn.year("date_closestObs_before"))\
                .withColumn("date_closestObs_after_weekOfYear", fn.weekofyear("date_closestObs_after"))\
                    .withColumn("date_closestObs_after_month", fn.month("date_closestObs_after"))\
                        .withColumn("date_closestObs_after_year", fn.year("date_closestObs_after"))



# Adjust for inconsistencies close to end/start of years, useful for building unique week and biweek indices.
UserSegment = UserSegment\
    .withColumn("start_year_adj", fn.when(((col("start_weekOfYear") == 1) & (col("start_month") == 12)), col("start_year")+1)
                .otherwise(fn.when(((col("start_weekOfYear") > 50) & (col("start_month") == 1)), col("start_year")-1)
                           .otherwise(col("start_year"))))\
    .withColumn("end_year_adj", fn.when(((col("end_weekOfYear") == 1) & (col("end_month") == 12)), col("end_year")+1)
                .otherwise(fn.when(((col("end_weekOfYear") > 50) & (col("end_month") == 1)), col("end_year")-1).otherwise(col("end_year"))))

UserSegment = UserSegment\
    .withColumn("date_closestObs_before_year_adj", fn.when(((col("date_closestObs_before_weekOfYear") == 1) & (col("date_closestObs_before_month") == 12)), col("date_closestObs_before_year")+1)
                .otherwise(fn.when(((col("date_closestObs_before_weekOfYear") > 50) & (col("date_closestObs_before_month") == 1)), col("date_closestObs_before_year")-1)
                           .otherwise(col("date_closestObs_before_year"))))\
    .withColumn("date_closestObs_after_year_adj", fn.when(((col("date_closestObs_after_weekOfYear") == 1) & (col("date_closestObs_after_month") == 12)), col("date_closestObs_after_year")+1)
                .otherwise(fn.when(((col("date_closestObs_after_weekOfYear") > 50) & (col("date_closestObs_after_month") == 1)), col("date_closestObs_after_year")-1).otherwise(col("date_closestObs_after_year"))))

# Calculate the day of the week for segment start and end dates (Monday is 1)
UserSegment = UserSegment\
    .withColumn("start_date_DayOfWeek", ((fn.dayofweek('start_date')+5) % 7)+1)\
        .withColumn("end_date_DayOfWeek", ((fn.dayofweek('end_date')+5) % 7)+1)

# Calculate dummies indicating whether segments start or end on intermediary or final entry and exit dates respectively.
# Infer gap in days with previous and following segments. Null values indicate final start and exit dates respectively.
UserSegment = UserSegment\
    .withColumn("gap_lag", fn.datediff(col("start_date"), col("date_closestObs_before"))-1)\
        .withColumn("gap_lead", fn.datediff(col("date_closestObs_after"), col("end_date"))-1)

# Rename segment_location and resid_seg_loc as destination_loc and origin_loc respectively
UserSegment = UserSegment\
    .withColumnRenamed("segment_location", "destination_loc")\
        .withColumnRenamed("resid_loc_seg", "origin_loc")


# Create unique segment start and segment end week/half-month/month indices by concatenating the corresponding week/half-month/month of year and year. Cast to integer type.
UserSegment = UserSegment\
    .withColumn("start_week_unique", fn.concat(col("start_year_adj"), fn.when(col("start_weekOfYear") < 10, "0").otherwise(""), col("start_weekOfYear").cast(StringType())))\
        .withColumn("end_week_unique", fn.concat(col("end_year_adj"), fn.when(col("end_weekOfYear") < 10, "0").otherwise(""), col("end_weekOfYear").cast(StringType())))\
            .withColumn("start_month_unique", fn.concat(col("start_year"), fn.when(col("start_month") < 10, "0").otherwise(""), col("start_month").cast(StringType())))\
                .withColumn("end_month_unique", fn.concat(col("end_year"), fn.when(col("end_month") < 10, "0").otherwise(""), col("end_month").cast(StringType())))\
                    .withColumn("start_halfMonth_unique", fn.concat(col("start_month_unique").cast(StringType()), fn.when(fn.dayofmonth('start_date') < 16, "01").otherwise("16")))\
                        .withColumn("end_halfMonth_unique", fn.concat(col("end_month_unique").cast(StringType()), fn.when(fn.dayofmonth('end_date') < 16, "01").otherwise("16")))\
                            .drop("start_year_adj","end_year_adj","start_weekOfYear","end_weekOfYear","start_year","end_year")

UserSegment = UserSegment\
    .withColumn("start_week_unique", col("start_week_unique").cast(IntegerType()))\
        .withColumn("end_week_unique", col("end_week_unique").cast(IntegerType()))\
            .withColumn("start_month_unique", col("start_month_unique").cast(IntegerType()))\
                .withColumn("end_month_unique", col("end_month_unique").cast(IntegerType()))\
                    .withColumn("start_halfMonth_unique", col("start_halfMonth_unique").cast(IntegerType()))\
                        .withColumn("end_halfMonth_unique", col("end_halfMonth_unique").cast(IntegerType()))

# Same thing for closest dates before and after segments
UserSegment = UserSegment\
    .withColumn("date_closestObs_before_week_unique", fn.concat(col("date_closestObs_before_year_adj"), fn.when(col("date_closestObs_before_weekOfYear") < 10, "0").otherwise(""), col("date_closestObs_before_weekOfYear").cast(StringType())))\
        .withColumn("date_closestObs_after_week_unique", fn.concat(col("date_closestObs_after_year_adj"), fn.when(col("date_closestObs_after_weekOfYear") < 10, "0").otherwise(""), col("date_closestObs_after_weekOfYear").cast(StringType())))\
            .withColumn("date_closestObs_before_month_unique", fn.concat(col("date_closestObs_before_year"), fn.when(col("date_closestObs_before_month") < 10, "0").otherwise(""), col("date_closestObs_before_month").cast(StringType())))\
                .withColumn("date_closestObs_after_month_unique", fn.concat(col("date_closestObs_after_year"), fn.when(col("date_closestObs_after_month") < 10, "0").otherwise(""), col("date_closestObs_after_month").cast(StringType())))\
                    .withColumn("date_closestObs_before_halfMonth_unique", fn.concat(col("date_closestObs_before_month_unique").cast(StringType()), fn.when(fn.dayofmonth('date_closestObs_before') < 16, "01").otherwise("16")))\
                        .withColumn("date_closestObs_after_halfMonth_unique", fn.concat(col("date_closestObs_after_month_unique").cast(StringType()), fn.when(fn.dayofmonth('date_closestObs_after') < 16, "01").otherwise("16")))

UserSegment = UserSegment\
    .withColumn("date_closestObs_before_week_unique", col("date_closestObs_before_week_unique").cast(IntegerType()))\
        .withColumn("date_closestObs_after_week_unique", col("date_closestObs_after_week_unique").cast(IntegerType()))\
            .withColumn("date_closestObs_before_month_unique", col("date_closestObs_before_month_unique").cast(IntegerType()))\
                .withColumn("date_closestObs_after_month_unique", col("date_closestObs_after_month_unique").cast(IntegerType()))\
                    .withColumn("date_closestObs_before_halfMonth_unique", col("date_closestObs_before_halfMonth_unique").cast(IntegerType()))\
                        .withColumn("date_closestObs_after_halfMonth_unique", col("date_closestObs_after_halfMonth_unique").cast(IntegerType()))


# Write and re-import UserSegment for computation optimization purposes since UserSegment is subsequently called on distinct operations
UserSegment.write.mode("overwrite").parquet(result_dir+"temp/UserSegment")

UserSegment = spark.read.parquet(result_dir+"temp/UserSegment")

for col_name in [value for value in UserSegment.columns if (value not in ["device_id", "start_date", "end_date", "date_closestObs_after", "date_closestObs_before"])]:
    UserSegment = UserSegment\
        .withColumn(col_name, col(col_name).cast(IntegerType()))

UserSegment = UserSegment\
    .withColumn("start_date", col("start_date").cast(DateType()))\
        .withColumn("end_date", col("end_date").cast(DateType()))\
            .withColumn("date_closestObs_after", col("date_closestObs_after").cast(DateType()))\
                .withColumn("date_closestObs_before", col("date_closestObs_before").cast(DateType()))


###
### Temporary migration departures and returns by week
###


if run_week:
    ## Loop on minimum migration duration parameter T_temp
    for T_temp in T_meso_min:
        ##__ Departures
        ## Initialize from UserSegment dataset and a constant column equal to eps_tol (tolerance parameter)
        MigrationDepartByTimeUnit=UserSegment\
            .select("device_id", "start_date", "end_date", "start_date_DayOfWeek", "date_closestObs_before_week_unique","date_closestObs_before","date_closestObs_after", "start_week_unique", "gap_lead", "origin_loc", "destination_loc", "loc_closestObs_before", "minDuration")\
                .withColumn("eps_tol",fn.lit(eps_tol))
                
        
        ## Calculate the limit date of observation before a segment according to eps_tol
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("limit_date_closestObs_before", fn.expr("date_sub(start_date,eps_tol+start_date_DayOfWeek)"))
                
        
        ## Calculate dummy equal to 1 if the start date of the segment correspond to a migration departure (high confidence, i.e. lower-bound estimate)
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("TempMigrDepart_dummy_week_highConf", fn.when(((col("minDuration") >= T_temp)) & ((fn.datediff(col("date_closestObs_before"), col("limit_date_closestObs_before"))) >= 0), 1).otherwise(0))
        
        
        ## Calculate dummy equal to 1 if the start date of the segment correspond to a migration departure (low confidence, i.e. upper-bound estimate)
        # Calculate the minimum date at which the segment could have started if it started in the same week as the start date.
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("min_start_date",fn.when(col("loc_closestObs_before")!=col("destination_loc"),\
                                                 fn.when(col("date_closestObs_before_week_unique")!=col("start_week_unique"),\
                                                         fn.expr("date_sub(start_date,start_date_DayOfWeek-1)"))\
                                                     .otherwise(fn.date_add(col("date_closestObs_before"),1)))\
                        .otherwise(fn.when((fn.datediff(col("start_date"),col("date_closestObs_before"))-fn.lit(1)-col("start_date_DayOfWeek"))>eps_gap_meso,\
                                           fn.expr("date_sub(start_date,start_date_DayOfWeek-1)"))\
                                   .otherwise(fn.date_add("date_closestObs_before",eps_gap_meso+2))))
        
        # Calculate the maximum end at which the segment could have ended.
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("max_end_date",fn.when(col("date_closestObs_after").isNull(),col("end_date")).otherwise(fn.date_sub("date_closestObs_after",1)))
        
        # Calculate dummy
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("TempMigrDepart_dummy_week_lowConf", fn.when((((fn.datediff(col("max_end_date"),col("min_start_date"))+1) >= T_temp)) & ((fn.datediff(col("date_closestObs_before"), col("limit_date_closestObs_before"))) >= 0), 1).otherwise(0))
        
        
        ## Aggregate departures by origin-destination-week
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .groupBy("start_week_unique", "origin_loc", "destination_loc")\
                .agg(fn.sum("TempMigrDepart_dummy_week_highConf").alias("N_depart_highConf"),
                     fn.sum("TempMigrDepart_dummy_week_lowConf").alias("N_depart_lowConf"))\
                    .withColumnRenamed("start_week_unique", "WeekIndex")
        
        
        ##__ Returns
        ## Initialize from UserSegment dataset and a constant column equal to eps_tol (tolerance parameter)
        MigrationReturnByTimeUnit=UserSegment\
            .select("device_id", "start_date", "end_date", "end_date_DayOfWeek", "date_closestObs_after_week_unique","date_closestObs_before","date_closestObs_after", "end_week_unique", "origin_loc", "destination_loc", "loc_closestObs_after", "minDuration")\
                .withColumn("eps_tol",fn.lit(eps_tol))
                
        
        ## Calculate the limit date of observation after a segment according to eps_tol
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("limit_date_closestObs_after", fn.expr("date_add(end_date,8-end_date_DayOfWeek+eps_tol)"))
        
        
        ## Calculate dummy equal to 1 if the end date of the segment correspond to a migration return (high confidence, i.e. lower-bound estimate)
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("TempMigrReturn_dummy_week_highConf", fn.when(((col("minDuration") >= T_temp)) & ((fn.datediff(col("limit_date_closestObs_after"), col("date_closestObs_after"))) >= 0), 1).otherwise(0))
        
        
        ## Calculate dummy equal to 1 if the end date of the segment correspond to a migration return (low confidence, i.e. upper-bound estimate)
        # Calculate the minimum date at which the segment could have started.
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("min_start_date",fn.when(col("date_closestObs_before").isNull(),col("start_date")).otherwise(fn.date_add("date_closestObs_before",1)))
        
        # Calculate the maximum end at which the segment could have ended.
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("max_end_date",fn.when(col("loc_closestObs_after")!=col("destination_loc"),
                                               fn.when(col("date_closestObs_after_week_unique")!=col("end_week_unique"),
                                                       fn.expr("date_add(end_date,7-end_date_DayOfWeek)")).otherwise(fn.date_sub("date_closestObs_after",1))).otherwise(fn.when(fn.datediff(col("date_closestObs_after"),fn.expr("date_add(end_date,7-end_date_DayOfWeek)"))>(eps_gap_meso+1),
                                                                                                                                                                                fn.expr("date_add(end_date,7-end_date_DayOfWeek)")).otherwise(fn.date_sub("date_closestObs_after",eps_gap_meso+2))))
        
        # Calculate dummy
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("TempMigrReturn_dummy_week_lowConf", fn.when((((fn.datediff(col("max_end_date"),col("min_start_date"))+1) >= T_temp)) & ((fn.datediff(col("limit_date_closestObs_after"), col("date_closestObs_after"))) >= 0), 1).otherwise(0))
            
        
        ## Aggregate returns by origin-destination-week
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .groupBy("end_week_unique", "origin_loc", "destination_loc")\
                .agg(fn.sum("TempMigrReturn_dummy_week_highConf").alias("N_return_highConf"),
                     fn.sum("TempMigrReturn_dummy_week_lowConf").alias("N_return_lowConf"))\
                    .withColumnRenamed("end_week_unique", "WeekIndex")
        
        
        ## Join migration flows (departures and returns) by week
        MigrationFlowByTimeUnit = MigrationDepartByTimeUnit\
            .join(MigrationReturnByTimeUnit, on=["WeekIndex", "origin_loc", "destination_loc"], how="full")\
                .na.fill(0)\
                    .orderBy(["origin_loc", "destination_loc", "WeekIndex"])
                    
        
        ## Export
        MigrationFlowByTimeUnit.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationFlow_"+str(T_temp)+"days_ByODWeek",header="true",mode="overwrite")


###
### Temporary migration departures and returns by half-month
###


if run_halfMonth:
    ## Loop on minimum migration duration parameter T_temp
    for T_temp in T_meso_min:
        ##__ Departures
        ## Initialize from UserSegment dataset and a constant column equal to eps_tol (tolerance parameter)
        MigrationDepartByTimeUnit=UserSegment\
            .select("device_id", "start_date", "end_date", "date_closestObs_before_halfMonth_unique","date_closestObs_before","date_closestObs_after", "start_halfMonth_unique", "origin_loc", "destination_loc", "loc_closestObs_before", "minDuration")\
                .withColumn("eps_tol",fn.lit(eps_tol))
                
        
        ## Calculate the limit date of observation before a segment according to eps_tol
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("start_date_DayOfHalfMonth",fn.when(fn.dayofmonth("start_date")<=15,fn.dayofmonth("start_date")).otherwise(fn.dayofmonth("start_date")-15))
        
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("limit_date_closestObs_before", fn.expr("date_sub(start_date,eps_tol+start_date_DayOfHalfMonth)"))
                

        ## Calculate dummy equal to 1 if the start date of the segment correspond to a migration departure (high confidence, i.e. lower-bound estimate)
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("TempMigrDepart_dummy_halfMonth_highConf", fn.when(((col("minDuration") >= T_temp)) & ((fn.datediff(col("date_closestObs_before"), col("limit_date_closestObs_before"))) >= 0), 1).otherwise(0))
        
        
        ## Calculate dummy equal to 1 if the start date of the segment correspond to a migration departure (low confidence, i.e. upper-bound estimate)
        # Calculate the minimum date at which the segment could have started if it started in the same half-month as the start date.
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("min_start_date",fn.when(col("loc_closestObs_before")!=col("destination_loc"),\
                                                 fn.when(col("date_closestObs_before_halfMonth_unique")!=col("start_halfMonth_unique"),\
                                                         fn.expr("date_sub(start_date,start_date_DayOfHalfMonth-1)"))\
                                                     .otherwise(fn.date_add(col("date_closestObs_before"),1)))\
                        .otherwise(fn.when((fn.datediff(col("start_date"),col("date_closestObs_before"))-fn.lit(1)-col("start_date_DayOfHalfMonth"))>eps_gap_meso,\
                                           fn.expr("date_sub(start_date,start_date_DayOfHalfMonth-1)"))\
                                   .otherwise(fn.date_add("date_closestObs_before",eps_gap_meso+2))))
        
        # Calculate the maximum end at which the segment could have ended.
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("max_end_date",fn.when(col("date_closestObs_after").isNull(),col("end_date")).otherwise(fn.date_sub("date_closestObs_after",1)))
        
        # Calculate dummy
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .withColumn("TempMigrDepart_dummy_halfMonth_lowConf", fn.when((((fn.datediff(col("max_end_date"),col("min_start_date"))+1) >= T_temp)) & ((fn.datediff(col("date_closestObs_before"), col("limit_date_closestObs_before"))) >= 0), 1).otherwise(0))
        
        
        ## Aggregate departures by origin-destination-halfmonth
        MigrationDepartByTimeUnit=MigrationDepartByTimeUnit\
            .groupBy("start_halfMonth_unique", "origin_loc", "destination_loc")\
                .agg(fn.sum("TempMigrDepart_dummy_halfMonth_highConf").alias("N_depart_highConf"),
                     fn.sum("TempMigrDepart_dummy_halfMonth_lowConf").alias("N_depart_lowConf"))\
                    .withColumnRenamed("start_halfMonth_unique", "HalfMonthIndex")
            
        
        ##__ Returns
        ## Initialize from UserSegment dataset and a constant column equal to eps_tol (tolerance parameter)
        MigrationReturnByTimeUnit=UserSegment\
            .select("device_id", "start_date", "end_date", "date_closestObs_after_halfMonth_unique","date_closestObs_before","date_closestObs_after", "end_halfMonth_unique", "origin_loc", "destination_loc", "loc_closestObs_after", "minDuration")\
                .withColumn("eps_tol",fn.lit(eps_tol))
                
        
        ## Calculate the limit date of observation after a segment according to eps_tol
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("end_date_DayOfHalfMonth",fn.when(fn.dayofmonth("end_date")<=15,fn.dayofmonth("end_date")).otherwise(fn.dayofmonth("end_date")-15))\
                .withColumn("end_date_NDaysInHalfMonth",fn.when(fn.dayofmonth("end_date")<=15,fn.lit(15)).otherwise(fn.dayofmonth(fn.last_day("end_date"))-15))
        

        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("limit_date_closestObs_after", fn.expr("date_add(end_date,end_date_NDaysInHalfMonth+1-end_date_DayOfHalfMonth+eps_tol)"))
        
        
        ## Calculate dummy equal to 1 if the end date of the segment correspond to a migration return (high confidence, i.e. lower-bound estimate)
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("TempMigrReturn_dummy_halfMonth_highConf", fn.when(((col("minDuration") >= T_temp)) & ((fn.datediff(col("limit_date_closestObs_after"), col("date_closestObs_after"))) >= 0), 1).otherwise(0))
        
        
        ## Calculate dummy equal to 1 if the end date of the segment correspond to a migration return (low confidence, i.e. upper-bound estimate)
        # Calculate the minimum date at which the segment could have started.
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("min_start_date",fn.when(col("date_closestObs_before").isNull(),col("start_date")).otherwise(fn.date_add("date_closestObs_before",1)))
        
        # Calculate the maximum end at which the segment could have ended.
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("max_end_date",fn.when(col("loc_closestObs_after")!=col("destination_loc"),
                                               fn.when(col("date_closestObs_after_halfMonth_unique")!=col("end_halfMonth_unique"),
                                                       fn.expr("date_add(end_date,end_date_NDaysInHalfMonth-end_date_DayOfHalfMonth)")).otherwise(fn.date_sub("date_closestObs_after",1))).otherwise(fn.when(fn.datediff(col("date_closestObs_after"),fn.expr("date_add(end_date,end_date_NDaysInHalfMonth-end_date_DayOfHalfMonth)"))>(eps_gap_meso+1),
                                                                                                                                                                                fn.expr("date_add(end_date,end_date_NDaysInHalfMonth-end_date_DayOfHalfMonth)")).otherwise(fn.date_sub("date_closestObs_after",eps_gap_meso+2))))
        
        # Calculate dummy
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .withColumn("TempMigrReturn_dummy_halfMonth_lowConf", fn.when((((fn.datediff(col("max_end_date"),col("min_start_date"))+1) >= T_temp)) & ((fn.datediff(col("limit_date_closestObs_after"), col("date_closestObs_after"))) >= 0), 1).otherwise(0))
            
        
        ## Aggregate returns by origin-destination-halfmonth
        MigrationReturnByTimeUnit=MigrationReturnByTimeUnit\
            .groupBy("end_halfMonth_unique", "origin_loc", "destination_loc")\
                .agg(fn.sum("TempMigrReturn_dummy_halfMonth_highConf").alias("N_return_highConf"),
                     fn.sum("TempMigrReturn_dummy_halfMonth_lowConf").alias("N_return_lowConf"))\
                    .withColumnRenamed("end_halfMonth_unique", "HalfMonthIndex")
        
        
        ## Join migration flows (departures and returns) by half-month
        MigrationFlowByTimeUnit = MigrationDepartByTimeUnit\
            .join(MigrationReturnByTimeUnit, on=["HalfMonthIndex", "origin_loc", "destination_loc"], how="full")\
                .na.fill(0)\
                    .orderBy(["origin_loc", "destination_loc", "HalfMonthIndex"])
                    
        
        ## Export
        MigrationFlowByTimeUnit.write.option("delimiter",";").csv(result_dir+"TempMigrationStats"+("_"+filter_name if filter_dummy==1 else "")+"/OD-matrices/TempMigrationFlow_"+str(T_temp)+"days_ByODHalfMonth",header="true",mode="overwrite")
