# Deriving temporary migration statistics from mobile phone data - Code
The project contains a series of PySpark and R scripts allowing to process raw mobile phone data and produce temporary migration statistics disaggregated over time and across space.
The repository comprises seven .py files written in PySpark and one .R file. A brief description of each script is provided below.

IMPORTANT: SCRIPTS MUST BE EXECUTED IN THE ORDER IN WHICH THEY ARE NUMBERED.

For instance, you should run "1-MigrationDetection_part0_SciData.py" first, then "2-MigrationDetection_part1_SciData.py" etc...

All methodological details are provided in Blanchard & Rubrichi (2024).


# 1-MigrationDetection_part0_SciData.py
Main input: raw mobile phone dataset such as Call Detail Records (`MPdataset`)

Main output: inferred daily locations for each individual user/device (`daily_loc`)

This script imports a raw mobile phone dataset and calculates/exports daily locations for each user.
First, hourly locations are computed as the modal location observed during the corresponding one-hour time window. 
Then, daily locations are defined as the modal hourly location at night-time (6pm-8am) if the user is observed during that time interval, and the modal hourly location during daytime otherwise.

The script supports two formats for the input dataset: either a .csv or .parquet file. The script expects an input raw dataset with three columns: `device_id` is a generic name defining a unique device or a unique SIM card, `call_date` indicates the timestamp when the phone transaction (e.g. call, text...) occured, `antenna_id` is a unique identifier for the antenna that processed the phone transaction.

The user needs to adjust column names if they differ from expected names `device_id`, `call_date`, and `antenna_id`. When importing the dataset (lines 56-61), the script can be adjusted to:

    if dataset_format=="csv":
        MPdataset=spark.read.csv(MPdataset_path,header="true",sep=";")\
            .select("sim_id","date","antenna_id")\
            .withColumnRenamed("sim_id","device_id")\
            .withColumnRenamed("date","call_date")\
            .withColumnRenamed("antenna","antenna_id")
    elif dataset_format=="parquet":
        MPdataset=spark.read.parquet(MPdataset_path)\
            .select("sim_id","date","antenna_id")\
            .withColumnRenamed("sim_id","device_id")\
            .withColumnRenamed("date","call_date")\
            .withColumnRenamed("antenna","antenna_id")

The script assumes that locations are defined as voronoi cells and that an antenna-voronoi dictionnary has been constructed. 
Note that other types of locations can be considered (e.g. administrative units or other self-defined spatial units) in which case section 3 of the script must be adjusted accordingly.
Also note that the script could be applied to location data such as smartphone app location data, although this would require some adjustments in input columns imported (i.e. "longitude" and "latitude" instead of "antenna_id") and the construction of a correspondence between unique occurences of coordinates and spatial unit identifiers.

The script has an option to process only a subset of `device_id` values (lines 44-54). A one-column .csv "filter" dataset must be constructed prior to executing this script, with a column named `device_id_filter` providing the set of `device_id` values to keep in the compilation of `daily_loc` output.

# 2-MigrationDetection_part1_SciData.py
Main input: inferred daily locations for each individual user/device (`daily_loc` (DataFrame))

Main output: Home location by user for users with a unique home location (`residLoc_unique` (DataFrame)) and monthly home location for users with multiple home locations over the observation period (`residLoc_multiple` (DataFrame))

The script imports daily locations, calculates monthly locations, and applies a macro-segment detection procedure to identify users' home location(s).

# 3-MigrationDetection_part2_SciData.py
Main input: inferred daily locations for each individual user/device (`daily_loc` (DataFrame)), Home locations datasets (`residLoc_unique` (DataFrame),`residLoc_multiple` (DataFrame))

Main output: dataset where each row represents a user-(meso-segment) detected (`UserSegment` (DataFrame))

The script imports daily locations and applies a clustering procedure to detect users/devices' meso-segments with a maximum duration of at least some specified threshold (default is left to 20 days).

# 4-ObservationGapDetection_SciData.py
