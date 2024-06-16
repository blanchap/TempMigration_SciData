# Deriving temporary migration statistics from mobile phone data - Code
The project contains a series of PySpark and R scripts allowing to process raw mobile phone data and produce temporary migration statistics disaggregated over time and across space.
The repository comprises seven .py files written in PySpark and one .R file. A brief description of each script is provided below.

IMPORTANT: SCRIPTS MUST BE EXECUTED IN THE ORDER IN WHICH THEY ARE NUMBERED.

For instance, you should run "1-MigrationDetection_part0_SciData.py" first, then "2-MigrationDetection_part1_SciData.py" etc...

All methodological details are provided in Blanchard & Rubrichi (2024):

Blanchard, P. and S. Rubrichi (2024): “A Highly Granular Temporary Migration Dataset Derived From Mobile Phone Data in Senegal”

All PySpark scripts start by creating a Spark session with some values assigned to properties related to memory management (`spark.driver.memory`,`spark.executor.memory`,`spark.driver.maxResultSize`). Users will note that those values should be adjusted to their specific needs depending on the size of the dataset to process and the characteristics of their machine. Details on how to set up a Spark session can be found [here](https://spark.apache.org/docs/latest/configuration.html).

# 1-MigrationDetection_part0.py
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

# 2-MigrationDetection_part1.py
Main input: inferred daily locations for each individual user/device (`daily_loc` (DataFrame))

Main output: Home location by user for users with a unique home location (`residLoc_unique` (DataFrame)) and monthly home location for users with multiple home locations over the observation period (`residLoc_multiple` (DataFrame))

The script imports daily locations, calculates monthly locations, and applies a macro-segment detection procedure to identify users' home location(s).

# 3-MigrationDetection_part2.py
Main input: inferred daily locations for each individual user/device (`daily_loc` (DataFrame)), Home locations datasets (`residLoc_unique` (DataFrame),`residLoc_multiple` (DataFrame))

Main output: dataset where each row represents a user-(meso-segment) detected (`UserSegment` (DataFrame))

The script imports daily locations and applies a clustering procedure to detect users/devices' meso-segments with a maximum duration of at least some specified threshold (default is left to 20 days).

# 4-ObservationGapDetection.py
Main input: inferred daily locations for each individual user/device (`daily_loc` (DataFrame)), Home locations datasets (`residLoc_unique` (DataFrame),`residLoc_multiple` (DataFrame))

Main output: dataset providing the observation gaps of all users (`ObsGap` (DataFrame))

The script is similar to script 3 and can be thought of as measuring the "complement" of user-segments. Each row of the output represents a user observation gap with its start date and its end date, with observation gaps being simply defined as groups of consecutive days without calls. Observation gaps are used as the primary input in script 5 to determine users' observation status for any given time unit.

# 5-MigrationStatistics_TimeDisaggregated_part1.py
Main input: Observation gap dataset (`ObsGap` (DataFrame))

Main output: Counts of users observed for departure, return and stock migration metrics by time unit and origin location

The script imports the dataset of user-level observation gaps and implements a set of algorithmic rules to infer the number of users observed by time unit for each migration metric (departure, return, stock). All methodological details are provided in Blanchard & Rubrichi (2024). In particular, illustrative diagrams with extensive comments are provided in the Supplementary Matrial to support the understanding of the code.

# 6-MigrationStatistics_TimeDisaggregated_part2.py
Main input: dataset of detected meso-segments for each individual user (`UserSegment` (DataFrame))

Main output: Migration flows (departures and returns) by origin-destination voronoi pair and time unit

The script imports meso-segments and calculates the number of temporary migration departures and returns by origin-destination voronoi pair and time unit. The type of time units considered can be half-months and/or weeks (ISO 8601 definition), by setting `run_halfMonth` and `run_week` to `True` respectively. Note that the script does not support the calculation of migration flows by calendar month yet, although the option may be implemented in future versions of the code. There is also an option to calculate migration flows associated with migration events of some minimum duration via the parameter `T_meso_min`. For instance, setting `T_meso_min=60` will allow to calculate for each (origin\*destination\*time) the number of departures in migration that lasted at least 60 days. `T_meso_min` can be a list of multiple values, in which case results for each individual value are exported in a separate file.

IMPORTANT: section 3 of the script calculates a number of useful variables in `UserSegment` DataFrame which are then used to calculate migration flows. The dataset is exported in a temporary folder (`result_dir+"temp/UserSegment"`) and re-used as input in script 7. The filtering parameters (`filter_dummy`,`filter_name`) specified in script 6 are thus automatically applied in script 7.

# 7-MigrationStatistics_TimeDisaggregated_part3.py
Main input: dataset of detected meso-segments for each individual user (`UserSegment` (DataFrame)) augmented with some useful variables and exported in a temporary folder in script 6

Main output: Migration stock by origin-destination voronoi pair and time unit

Similar to script 6, script 7 allows to calculate migration stock values by origin-destination voronoi pair and time unit.

# 8-MigrationDatasetBuilder.R
Main input: outputs from scripts 5-7

Main output: temporary migration statistics 

Script 8 is an R script that allows to aggregate voronoi-level migration estimates calculated in scripts 5-7 to produce temporary migration statistics at a specified spatio-temporal resolution. In particular, the script implements the weighting scheme described in Blanchard & Rubrichi (2024) which corrects for imbalances in the spatial distribution of phone users across strata relative to the distribution of some target population.

Note that a voronoi shapefile defining the voronoi cells considered in scripts 1-7 must be available at a path defined by parameter `voronoi_path`. The user must also construct a correspondence table assigning each voronoi cell to a stratum, where strata are the spatial units underlying the weighting scheme (see Blanchard & Rubrichi (2024) for further details). Such table must contain the following column names: "voronoi_id", "stratum_id", "targetPop"; defining the identifier of voronoi cells, a stratum identifier, and the value of the target population in the corresponding voronoi cell.
