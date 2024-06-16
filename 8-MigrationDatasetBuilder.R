#~~~~~~~~~~~~~~~~~~~~~~~~~~~#
# Migration Dataset builder #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~#


###
#0# Parameters and libraries ====
###


##__ Load libraries
for(package_name in c("data.table","sf","plyr","dplyr","doParallel","stringr","splitstackshape","R.utils")){
  ## If the package is not installed, install it
  if(eval(parse(text=paste("require(",package_name,")")))==F){
    install.packages(package_name)
  }
  
  ## Load package
  eval(parse(text=paste("require(",package_name,")")))
  if(package_name=="sf"){
    sf_use_s2(FALSE) #Switch off spherical geometry
  }
}


##__ Specify some paths
## Path pointing to the results folder where outputs from this scripts 5-7 were exported
result_dir=".../"

## Path pointing to the voronoi shapefile
voronoi_path="....gpkg"

## Path pointing to a .csv correspondence table mapping voronoi cell identifiers to strata identifiers defining weighting units. The table also contains voronoi-level estimates of the target population (e.g. population over 15).
## Note: This table must be constructed by the user prior to running this script.
stratum_path="....csv"


##__ Import voronoi shapefile
## Import
voronoi_shp=st_read(dsn=paste0(voronoi_path),
                    layer="specify layer name here")

## Store the attribute table in a separate data.table object
voronoiTab=voronoi_shp%>%
  st_set_geometry(NULL)%>%
  setDT()

## Construct a variable defining the spatial units for which aggregate statistics are produced.
## Note: These spatial units are specifically designed for the publication of the Senegal temporary migration dataset, but other applications can reuse the code considering a distinct level of aggregation that the user can define.

# Reformat city_name to avoid ambiguous characters
voronoiTab[,city_name:=iconv(city_name,from="UTF-8",to="ASCII//TRANSLIT")]

# Create a simple city identifier
all_city_names=voronoiTab[!is.na(city_name),sort(city_name)]
all_city_id=paste0("city-",sprintf("%03d",1:length(all_city_names)))
voronoiTab[,city_id:=mapvalues(city_name,
                               from=all_city_names,
                               to=all_city_id)]

# Create an identifier corresponding to the city identifier if the voronoi cell is classified as urban, and to the adm3 unit identifier to which it belongs if it is rural.
voronoiTab[,id:=ifelse(urban_dummy_WP==1,city_id,
                       paste0(GID_3_adjusted,"-rural"))]

# Similarly, create a name for each spatial unit defined.
voronoiTab[,name:=ifelse(urban_dummy_WP==1,city_name,
                         paste0(NAME_3_adjusted,"-rural"))]


##__ Import the voronoi-stratum correspondence table
## Must contain the following columns: "voronoi_id", "stratum_id", "targetPop"
stratum_ByVoronoi=fread(paste0(stratum_path))


##__ Functions to import a dataset saved as a set of parquet files or csv files in a folder
readPARQUETFolder=function(parquet_folder_path){
  filenames_temp=list.files(parquet_folder_path,full.names = T)
  filenames_temp=filenames_temp[!grepl(pattern = ".parquet.crc|SUCCESS",filenames_temp)]
  output=foreach(f=filenames_temp,
                 .combine=function(...){rbindlist(list(...))},
                 .multicombine = TRUE,
                 .inorder = T)%do%{
                   output_temp=tryCatch({
                     read_parquet(f)
                   }, error=function(err){NULL})
                   setDT(output_temp)
                   output_temp
                 }
  return(output)
}

readCSVFolder=function(csv_folder_path){
  filenames_temp=list.files(csv_folder_path,full.names = T)
  filenames_temp=filenames_temp[grepl(pattern = "csv$",filenames_temp)]
  output=foreach(f=filenames_temp,
                 .combine=function(...){rbindlist(list(...))},
                 .multicombine = TRUE,
                 .inorder = T)%do%{
                   output_temp=fread(f)
                   output_temp
                 }
  return(output)
}


###
### Create full sequences of time units for the time period considered ====
###


##__ Specify manually the start and end time unit indices of the period to consider
# Week indices are integers of the form yyyyww (with ww in 01-53 according to ISO 8601)
min_week=201401
max_week=201601

# Half-month indices are integers of the form yyyyMM01 (for the first half of month MM of year yyyy) or yyyyMM16 (for the second half of month MM of year yyyy)
min_halfMonth=20140101
max_halfMonth=20151216

# Month indices are integers of the form yyyyMM
min_month=201401
max_month=201512


##__ Create sequences of time units
# Full sequences of time units from 2010 to 2026
full_week_sequence = c(seq(201001, 201052, 1),
                       seq(201101, 201152, 1),
                       seq(201201, 201252, 1),
                       seq(201301, 201352, 1),
                       seq(201401, 201452, 1),
                       seq(201501, 201553, 1),
                       seq(201601, 201652, 1),
                       seq(201701, 201752, 1),
                       seq(201801, 201852, 1),
                       seq(201901, 201952, 1),
                       seq(202001, 202053, 1),
                       seq(202101, 202152, 1),
                       seq(202201, 202252, 1),
                       seq(202301, 202352, 1),
                       seq(202401, 202452, 1),
                       seq(202501, 202552, 1),
                       seq(202601, 202652, 1))
full_week_sequence=full_week_sequence[full_week_sequence>= min_week & full_week_sequence<=max_week]

full_month_sequence = c(seq(201001, 201012, 1),
                        seq(201101, 201112, 1),
                        seq(201201, 201212, 1),
                        seq(201301, 201312, 1),
                        seq(201401, 201412, 1),
                        seq(201501, 201512, 1),
                        seq(201601, 201612, 1),
                        seq(201701, 201712, 1),
                        seq(201801, 201812, 1),
                        seq(201901, 201912, 1),
                        seq(202001, 202012, 1),
                        seq(202101, 202112, 1),
                        seq(202201, 202212, 1),
                        seq(202301, 202312, 1),
                        seq(202401, 202412, 1),
                        seq(202501, 202512, 1),
                        seq(202601, 202612, 1))
full_month_sequence=full_month_sequence[full_month_sequence>= min_month & full_month_sequence<=max_month]

full_halfMonth_sequence=sort(as.numeric(
  c(paste0(full_month_sequence,"01"),
    paste0(full_month_sequence,"16"))))


###
### Functions allowing to build weight tables ====
###


##__ By half-month for migration flow
## Arguments:
#   - T_meso_min: (numeric or character) Minimum duration threshold considered for a meso-segment to be classified as a temporary migration event

WeightTableBuilder_ByHalfMonth_flow=function(T_meso_min){
  ## Import tables with number of users observed for departures and returns by half-month and origin location
  N_users_ByOHalfMonth_flow=list()
  N_users_ByOHalfMonth_flow[["depart"]]=readCSVFolder(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/OD-matrices/N_users_ByOriginHalfMonth_",T_meso_min,"days_depart"))
  N_users_ByOHalfMonth_flow[["return"]]=readCSVFolder(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/OD-matrices/N_users_ByOriginHalfMonth_",T_meso_min,"days_return"))
  
  ## Transform to long format
  for(flow_type in c("depart","return")){
    N_users_ByOHalfMonth_flow[[flow_type]]=melt.data.table(N_users_ByOHalfMonth_flow[[flow_type]],
                                                           id.vars = "origin_loc",
                                                           measure.vars = names(N_users_ByOHalfMonth_flow[[flow_type]])[grepl("N_users_observed",names(N_users_ByOHalfMonth_flow[[flow_type]]))],
                                                           variable.name = "HalfMonthIndex",
                                                           value.name = paste0("N_users_observed_",flow_type))
    N_users_ByOHalfMonth_flow[[flow_type]][,HalfMonthIndex:=as.numeric(str_remove_all(HalfMonthIndex,"N_users_observed_"))]
  }
  
  ## Bind and join: final table in long format with N_users_observed_depart and N_users_observed_return
  N_users_ByOHalfMonth_flow=full_join(N_users_ByOHalfMonth_flow[["depart"]],N_users_ByOHalfMonth_flow[["return"]])
  
  N_users_ByOHalfMonth_flow[is.na(N_users_observed_depart),N_users_observed_depart:=0]
  N_users_ByOHalfMonth_flow[is.na(N_users_observed_return),N_users_observed_return:=0]
  
  
  ## Add missing location-time units
  fullTable_temp=expandRows(data.table(origin_loc=voronoiTab[,voronoi_id]),
                            count=length(full_halfMonth_sequence),
                            count.is.col = F)
  N_loc=voronoiTab[,.N]
  fullTable_temp[,HalfMonthIndex:=rep(..full_halfMonth_sequence,..N_loc)]
  N_users_ByOHalfMonth_flow=left_join(fullTable_temp,N_users_ByOHalfMonth_flow)
  
  N_users_ByOHalfMonth_flow[is.na(N_users_observed_depart),N_users_observed_depart:=0]
  N_users_ByOHalfMonth_flow[is.na(N_users_observed_return),N_users_observed_return:=0]
  
  
  ## Join stratum identifier and target population estimates used for calculating weight
  col_keep=c("voronoi_id","stratum_id","targetPop")
  N_users_ByOHalfMonth_flow=left_join(N_users_ByOHalfMonth_flow,
                                      stratum_ByVoronoi[,..col_keep],
                                      by=c("origin_loc"="voronoi_id"))
  
  
  ## Aggregate at stratum-level
  N_users_ByWeightingUnitHalfMonth_flow=N_users_ByOHalfMonth_flow[,.(N_users_observed_depart=sum(N_users_observed_depart),
                                                                     N_users_observed_return=sum(N_users_observed_return),
                                                                     targetPop=sum(targetPop)),
                                                                  by=c("stratum_id","HalfMonthIndex")]
  
  
  ## Calculate weights
  weight_ByHalfMonth_flow=N_users_ByWeightingUnitHalfMonth_flow[,1:ncol(N_users_ByWeightingUnitHalfMonth_flow)]
  
  weight_ByHalfMonth_flow[,weight_depart:=targetPop/N_users_observed_depart]
  weight_ByHalfMonth_flow[,weight_return:=targetPop/N_users_observed_return]
  
  
  return(list(N_users_ByO_tab=N_users_ByOHalfMonth_flow,
              weightTab=weight_ByHalfMonth_flow))
}


##__ By half-month for migration stock
## Arguments:
#   - T_meso_min: (numeric or character) Minimum duration threshold considered for a meso-segment to be classified as a temporary migration event

WeightTableBuilder_ByHalfMonth_stock=function(T_meso_min){
  
  ## Import tables with number of users observed for migration status by half-month and origin location
  N_users_ByOHalfMonth_stock=readCSVFolder(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/OD-matrices/N_users_ByOriginHalfMonth_",T_meso_min,"days_status"))
  
  ## Transform to long format
  N_users_ByOHalfMonth_stock=melt.data.table(N_users_ByOHalfMonth_stock,
                                             id.vars = "origin_loc",
                                             measure.vars = names(N_users_ByOHalfMonth_stock)[grepl("N_users_observed",names(N_users_ByOHalfMonth_stock))],
                                             variable.name = "HalfMonthIndex",
                                             value.name = "N_users_observed_stock")
  N_users_ByOHalfMonth_stock[,HalfMonthIndex:=as.numeric(str_remove_all(HalfMonthIndex,"N_users_observed_"))]
  
  ## Add missing location-time units
  fullTable_temp=expandRows(data.table(origin_loc=voronoiTab[,voronoi_id]),
                            count=length(full_halfMonth_sequence),
                            count.is.col = F)
  N_loc=voronoiTab[,.N]
  fullTable_temp[,HalfMonthIndex:=rep(..full_halfMonth_sequence,..N_loc)]
  N_users_ByOHalfMonth_stock=left_join(fullTable_temp,N_users_ByOHalfMonth_stock)
  
  N_users_ByOHalfMonth_stock[is.na(N_users_observed_stock),N_users_observed_stock:=0]
  
  
  ## Join stratum identifier and target population estimates used for calculating weight
  col_keep=c("voronoi_id","stratum_id","targetPop")
  N_users_ByOHalfMonth_stock=left_join(N_users_ByOHalfMonth_stock,
                                       stratum_ByVoronoi[,..col_keep],
                                       by=c("origin_loc"="voronoi_id"))
  
  
  ## Aggregate at stratum-level
  N_users_ByWeightingUnitHalfMonth_stock=N_users_ByOHalfMonth_stock[,.(N_users_observed_stock=sum(N_users_observed_stock),
                                                                       targetPop=sum(targetPop)),
                                                                    by=c("stratum_id","HalfMonthIndex")]
  
  
  ## Calculate weights
  weight_ByHalfMonth_stock=N_users_ByWeightingUnitHalfMonth_stock[,1:ncol(N_users_ByWeightingUnitHalfMonth_stock)]
  
  weight_ByHalfMonth_stock[,weight_stock:=targetPop/N_users_observed_stock]
  
  
  return(list(N_users_ByO_tab=N_users_ByOHalfMonth_stock,
              weightTab=weight_ByHalfMonth_stock))
}


###
### Specify parameters for the calculation of aggregated migration statistics ====
###


## Specify the type of estimates, the underlying subset from which statistics are derived, the minimum duration defining temporary migration events, and the level of confidence for the identification of migration events ("high" or "low").
# Type of estimates: "weighted" or "unweighted"
estimate_type="weighted"

# Subset used, defined by the parameter "filter_name" used in scripts 1-7
filter_dummy=1
filter_name="B"

# Minimum duration threshold
T_meso_min=20 # in days

# Level of confidence associated with the identification of migration events.
# "high": only events with an observed duration greater than T_meso_min are considered
# "low": estimates also account for meso-segments with an observed duration lower than T_meso_min but a maximum duration greater than T_meso_min
eventIdent_conf="high"


## Specify the name of the variable in voronoiTab used for the aggregation
aggreg_var_name="id"


## Specify the type of time units used: either "Week", "HalfMonth", or "Month"
## Note: outputs of scripts 1-7 for the specified type of time unit must be available
timeUnit_type="HalfMonth"


## Option to specify a minimum number of observed users imposed for any given origin-destination-time in order to ensure anonymity.
anonym_thresh=0


###
### Migration flow statistics ====
###


##__ Import estimates of migration flows by origin-destination-time unit and reformat
## Import
migrationFlow_ByODTimeUnit=readCSVFolder(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/OD-matrices/TempMigrationFlow_",T_meso_min,"days_ByOD",timeUnit_type))

## Keep estimates for the specified level of confidence (parameter eventIdent_conf) and remove the corresponding suffix in variable names
var_name_keep=names(migrationFlow_ByODTimeUnit)[!grepl(pattern=paste0(ifelse(eventIdent_conf=="high","low","high"),"Conf"),
                                                       x=names(migrationFlow_ByODTimeUnit))]
migrationFlow_ByODTimeUnit=migrationFlow_ByODTimeUnit[,..var_name_keep]

names(migrationFlow_ByODTimeUnit)=str_remove_all(string = names(migrationFlow_ByODTimeUnit),
                                                 pattern=paste0("_",eventIdent_conf,"Conf"))


##__ Generate weight table, join weights and calculate adjusted estimates
## Execute the weight table builder function
WeightTableBuilder_output_temp=WeightTableBuilder_ByHalfMonth_flow(T_meso_min = T_meso_min)

if(estimate_type=="weighted"){
  
  ## Join the stratum_id to each origin location in migration table
  migrationFlow_ByODTimeUnit=left_join(migrationFlow_ByODTimeUnit,
                                       stratum_ByVoronoi[,c("voronoi_id","stratum_id")],
                                       by=c("origin_loc"="voronoi_id"))
  
  ## Join weights by stratum_id
  migrationFlow_ByODTimeUnit=left_join(migrationFlow_ByODTimeUnit,
                                       WeightTableBuilder_output_temp$weightTab[,c("stratum_id","HalfMonthIndex","weight_depart","weight_return")],
                                       by=c("stratum_id"="stratum_id","HalfMonthIndex"="HalfMonthIndex"))
  
  ## Calculate adjusted estimates (variable names will be adjusted by adding the "_adj" suffix before exporting results)
  migrationFlow_ByODTimeUnit[,N_depart:=N_depart*weight_depart]
  migrationFlow_ByODTimeUnit[,N_return:=N_return*weight_return]
}


##__ Aggregate at specified level
## Join the spatial unit identifier for the specified level at both origin and destination locations
col_keep=c("voronoi_id",aggreg_var_name)
migrationFlow_ByODTimeUnit=left_join(migrationFlow_ByODTimeUnit,
                                     voronoiTab[,..col_keep],
                                     by=c("origin_loc"="voronoi_id"))
setnames(migrationFlow_ByODTimeUnit,
         old=aggreg_var_name,
         new="aggreg_var_origin")

migrationFlow_ByODTimeUnit=left_join(migrationFlow_ByODTimeUnit,
                                     voronoiTab[,..col_keep],
                                     by=c("destination_loc"="voronoi_id"))
setnames(migrationFlow_ByODTimeUnit,
         old=aggreg_var_name,
         new="aggreg_var_destination")

## Aggregate
migrationFlow_final=migrationFlow_ByODTimeUnit[,
                                               .(N_depart=sum(N_depart,na.rm=T),
                                                 N_return=sum(N_return,na.rm=T)),
                                               by=c("aggreg_var_origin","aggreg_var_destination","HalfMonthIndex")]

## Add missing origin-destination-time if any (do not include origin-origin pairs)
all_aggreg_id=voronoiTab[,unique(get(aggreg_var_name))]

fullTable_temp=expandRows(data.table(aggreg_var_origin=all_aggreg_id),
                          count=length(all_aggreg_id),
                          count.is.col = F)
fullTable_temp[,aggreg_var_destination:=rep(..all_aggreg_id,length(..all_aggreg_id))]

fullTable_temp=expandRows(fullTable_temp,
                          count=length(full_halfMonth_sequence),
                          count.is.col = F)

fullTable_temp[,HalfMonthIndex:=rep(..full_halfMonth_sequence,length(..all_aggreg_id)^2)]

fullTable_temp=fullTable_temp[aggreg_var_origin!=aggreg_var_destination]

migrationFlow_final=left_join(fullTable_temp,migrationFlow_final)
# !Note: migration counts for missing rows that were added are left to NA for now. They will be set to 0 later on if the number of users observed at origin is effectively strictly positive.


##__ Calculate rates of departures and returns
## Calculate the number of observed users for departures and returns for "unweighted" estimates, and target population values for "weighted" estimates
# Join variable used for aggregation to voronoi-level stratum dataset (stratum_ByVoronoi)
col_keep=c("voronoi_id",aggreg_var_name)
stratum_ByVoronoi_temp=left_join(stratum_ByVoronoi,
                                 voronoiTab[,..col_keep])

# Group stratum_ByVoronoi by unique aggregation variable value to obtain a correspondence table giving the aggregation variable value for each stratum identifier
col_keep=c("stratum_id",aggreg_var_name)
aggVar_ByStratum=unique(stratum_ByVoronoi_temp,by=c("stratum_id"))[,..col_keep]

# Join aggregation variable to the table containing stratum-level counts of observed users and target population (2nd element of the weight table builder output WeightTableBuilder_output_temp)
col_keep=c("voronoi_id",aggreg_var_name)
WeightTableBuilder_output_temp$weightTab=left_join(WeightTableBuilder_output_temp$weightTab,
                                                   aggVar_ByStratum)
setnames(WeightTableBuilder_output_temp$weightTab,
         old=aggreg_var_name,
         new="aggreg_var_origin")

# Aggregate counts of observed users and target population by aggregate origin and time unit
# Note: Target population in strata with no users observed are excluded from the aggregation
N_users_ByaggOriginTimeUnit_flow=WeightTableBuilder_output_temp$weightTab[,
                                                                          .(N_users_observed_depart=sum(N_users_observed_depart,na.rm=T),
                                                                            N_users_observed_return=sum(N_users_observed_return,na.rm=T),
                                                                            targetPop_depart=sum(targetPop*as.numeric(N_users_observed_depart>0),na.rm=T),
                                                                            targetPop_return=sum(targetPop*as.numeric(N_users_observed_return>0),na.rm=T)
                                                                          ),
                                                                          by=c("aggreg_var_origin","HalfMonthIndex")]

## Join to migrationFlow_final
migrationFlow_final=left_join(migrationFlow_final,
                              N_users_ByaggOriginTimeUnit_flow)

## Set NA migration counts to 0 where the corresponding number of users observed is strictly positive
migrationFlow_final[is.na(N_depart) & N_users_observed_depart>0,N_depart:=0]
migrationFlow_final[is.na(N_return) & N_users_observed_return>0,N_return:=0]

## Calculate rate of departures and returns
if(estimate_type=="weighted"){
  migrationFlow_final[,rate_depart:=N_depart/targetPop_depart]
  migrationFlow_final[,rate_return:=N_return/targetPop_return]
}else if(estimate_type=="unweighted"){
  migrationFlow_final[,rate_depart:=N_depart/N_users_observed_depart]
  migrationFlow_final[,rate_return:=N_return/N_users_observed_return]
}


##__ Apply the anonymity threshold
migrationFlow_final[N_users_observed_depart<anonym_thresh,c("N_depart","rate_depart","N_users_observed_depart","targetPop_depart"):=NA]
migrationFlow_final[N_users_observed_return<anonym_thresh,c("N_return","rate_return","N_users_observed_return","targetPop_return"):=NA]


##__ Finalize dataset
## Simply rename origin and destination columns "origin" and "destination" respectively
setnames(migrationFlow_final,
         old=c("aggreg_var_origin","aggreg_var_destination"),
         new=c("origin","destination"))

## If weighted estimates were calculated, add "_adj" suffix to migration variable names
if(estimate_type=="weighted"){
  setnames(migrationFlow_final,
           old=c("N_depart","N_return","rate_depart","rate_return","targetPop_depart","targetPop_return"),
           new=c("N_depart_adj","N_return_adj","rate_depart_adj","rate_return_adj","N_users_observed_depart_adj","N_users_observed_return_adj"))
}

## Specify columns to keep
if(estimate_type=="weighted"){
  col_keep=c("origin","destination","HalfMonthIndex","N_depart_adj","N_return_adj","rate_depart_adj","rate_return_adj","N_users_observed_depart_adj","N_users_observed_return_adj")
}else if(estimate_type=="unweighted"){
  col_keep=c("origin","destination","HalfMonthIndex","N_depart","N_return","rate_depart","rate_return","N_users_observed_depart","N_users_observed_return")
}

migrationFlow_final=migrationFlow_final[,..col_keep]


###
### Migration stock statistics ====
###


##__ Import estimates of migration stock by origin-destination-time unit and reformat
## Import
migrationStock_ByODTimeUnit=readCSVFolder(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/OD-matrices/TempMigrationStock_",T_meso_min,"days_ByOD",timeUnit_type))

## Keep estimates for the specified level of confidence (parameter eventIdent_conf) and remove the corresponding suffix in variable names
var_name_keep=names(migrationStock_ByODTimeUnit)[!grepl(pattern=paste0(ifelse(eventIdent_conf=="high","low","high"),"Conf"),
                                                        x=names(migrationStock_ByODTimeUnit))]
migrationStock_ByODTimeUnit=migrationStock_ByODTimeUnit[,..var_name_keep]

names(migrationStock_ByODTimeUnit)=str_remove_all(string = names(migrationStock_ByODTimeUnit),
                                                  pattern=paste0("_",eventIdent_conf,"Conf"))


##__ Generate weight table, join weights and calculate adjusted estimates
## Execute the weight table builder function
WeightTableBuilder_output_temp=WeightTableBuilder_ByHalfMonth_stock(T_meso_min = T_meso_min)

if(estimate_type=="weighted"){
  
  ## Join the stratum_id to each origin location in migration table
  migrationStock_ByODTimeUnit=left_join(migrationStock_ByODTimeUnit,
                                        stratum_ByVoronoi[,c("voronoi_id","stratum_id")],
                                        by=c("origin_loc"="voronoi_id"))
  
  ## Join weights by stratum_id
  migrationStock_ByODTimeUnit=left_join(migrationStock_ByODTimeUnit,
                                        WeightTableBuilder_output_temp$weightTab[,c("stratum_id","HalfMonthIndex","weight_stock")],
                                        by=c("stratum_id"="stratum_id","HalfMonthIndex"="HalfMonthIndex"))
  
  ## Calculate adjusted estimates (variable names will be adjusted by adding the "_adj" suffix before exporting results)
  migrationStock_ByODTimeUnit[,N_migrants:=N_migrants*weight_stock]
}


##__ Aggregate at specified level
## Join the spatial unit identifier for the specified level at both origin and destination locations
col_keep=c("voronoi_id",aggreg_var_name)
migrationStock_ByODTimeUnit=left_join(migrationStock_ByODTimeUnit,
                                      voronoiTab[,..col_keep],
                                      by=c("origin_loc"="voronoi_id"))
setnames(migrationStock_ByODTimeUnit,
         old=aggreg_var_name,
         new="aggreg_var_origin")

migrationStock_ByODTimeUnit=left_join(migrationStock_ByODTimeUnit,
                                      voronoiTab[,..col_keep],
                                      by=c("destination_loc"="voronoi_id"))
setnames(migrationStock_ByODTimeUnit,
         old=aggreg_var_name,
         new="aggreg_var_destination")

## Aggregate
migrationStock_final=migrationStock_ByODTimeUnit[,
                                                 .(N_migrants=sum(N_migrants,na.rm=T)),
                                                 by=c("aggreg_var_origin","aggreg_var_destination","HalfMonthIndex")]

## Add missing origin-destination-time if any (do not include origin-origin pairs)
all_aggreg_id=voronoiTab[,unique(get(aggreg_var_name))]

fullTable_temp=expandRows(data.table(aggreg_var_origin=all_aggreg_id),
                          count=length(all_aggreg_id),
                          count.is.col = F)
fullTable_temp[,aggreg_var_destination:=rep(..all_aggreg_id,length(..all_aggreg_id))]

fullTable_temp=expandRows(fullTable_temp,
                          count=length(full_halfMonth_sequence),
                          count.is.col = F)

fullTable_temp[,HalfMonthIndex:=rep(..full_halfMonth_sequence,length(..all_aggreg_id)^2)]

fullTable_temp=fullTable_temp[aggreg_var_origin!=aggreg_var_destination]

migrationStock_final=left_join(fullTable_temp,migrationStock_final)
# !Note: migration counts for missing rows that were added are left to NA for now. They will be set to 0 later on if the number of users observed at origin is effectively strictly positive.


##__ Calculate migration rates (i.e. stock rates)
## Calculate the number of observed users for stock for "unweighted" estimates, and target population values for "weighted" estimates
# Join aggregation variable to the table containing stratum-level counts of observed users and target population (2nd element of the weight table builder output WeightTableBuilder_output_temp)
col_keep=c("voronoi_id",aggreg_var_name)
WeightTableBuilder_output_temp$weightTab=left_join(WeightTableBuilder_output_temp$weightTab,
                                                   aggVar_ByStratum)
setnames(WeightTableBuilder_output_temp$weightTab,
         old=aggreg_var_name,
         new="aggreg_var_origin")

# Aggregate counts of observed users and target population by aggregate origin and time unit
# Note: Target population in strata with no users observed are excluded from the aggregation
N_users_ByaggOriginTimeUnit_stock=WeightTableBuilder_output_temp$weightTab[,
                                                                           .(N_users_observed_stock=sum(N_users_observed_stock,na.rm=T),
                                                                             targetPop_stock=sum(targetPop*as.numeric(N_users_observed_stock>0),na.rm=T)
                                                                           ),
                                                                           by=c("aggreg_var_origin","HalfMonthIndex")]

## Join to migrationStock_final
migrationStock_final=left_join(migrationStock_final,
                               N_users_ByaggOriginTimeUnit_stock)

## Set NA migration counts to 0 where the corresponding number of users observed is strictly positive
migrationStock_final[is.na(N_migrants) & N_users_observed_stock>0,N_migrants:=0]

## Calculate migration rate
if(estimate_type=="weighted"){
  migrationStock_final[,rate_migrants:=N_migrants/targetPop_stock]
}else if(estimate_type=="unweighted"){
  migrationStock_final[,rate_migrants:=N_migrants/N_users_observed_stock]
}


##__ Apply the anonymity threshold
migrationStock_final[N_users_observed_stock<anonym_thresh,c("N_migrants","rate_migrants","N_users_observed_stock","targetPop_stock"):=NA]


##__Export
## Simply rename origin and destination columns "origin" and "destination" respectively
setnames(migrationStock_final,
         old=c("aggreg_var_origin","aggreg_var_destination"),
         new=c("origin","destination"))

## If weighted estimates were calculated, add "_adj" suffix to migration variable names
if(estimate_type=="weighted"){
  setnames(migrationStock_final,
           old=c("N_migrants","rate_migrants","targetPop_stock"),
           new=c("N_migrants_adj","rate_migrants_adj","N_users_observed_stock_adj"))
}

## Specify columns to keep
if(estimate_type=="weighted"){
  col_keep=c("origin","destination","HalfMonthIndex","N_migrants_adj","rate_migrants_adj","N_users_observed_stock_adj")
}else if(estimate_type=="unweighted"){
  col_keep=c("origin","destination","HalfMonthIndex","N_migrants","rate_migrants","N_users_observed_stock")
}

migrationStock_final=migrationStock_final[,..col_keep]


###
### Join migration flows and migration stock statistics and export ====
###


##__ Join
migration_final=left_join(migrationFlow_final,
                          migrationStock_final)


##__ Exclude time units at the boundaries of observation periods
if(timeUnit_type=="HalfMonth"){
  if(T_meso_min==20){
    migration_final=migration_final[!HalfMonthIndex%in%full_halfMonth_sequence[c(1,length(full_halfMonth_sequence))]]
  }else if(T_meso_min==30){
    migration_final=migration_final[!HalfMonthIndex%in%full_halfMonth_sequence[c(1:2,(length(full_halfMonth_sequence)-1):length(full_halfMonth_sequence))]]
  }else if(T_meso_min==60){
    migration_final=migration_final[!HalfMonthIndex%in%full_halfMonth_sequence[c(1:4,(length(full_halfMonth_sequence)-3):length(full_halfMonth_sequence))]]
  }
}else if(timeUnit_type=="Month"){
  if(T_meso_min%in%c(20,30)){
    migration_final=migration_final[!MonthIndex%in%full_month_sequence[c(1,length(full_month_sequence))]]
  }else if(T_meso_min==60){
    migration_final=migration_final[!MonthIndex%in%full_month_sequence[c(1:2,(length(full_month_sequence)-1):length(full_month_sequence))]]
  }
}


##__ Export as compressed csv
fwrite(migration_final,
       paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/",estimate_type,"_",ifelse(filter_dummy==1,filter_name,"FullData"),"_",T_meso_min,"days_ByOD",timeUnit_type,"_",eventIdent_conf,"Conf.csv"))
gzip(paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/",estimate_type,"_",ifelse(filter_dummy==1,filter_name,"FullData"),"_",T_meso_min,"days_ByOD",timeUnit_type,"_",eventIdent_conf,"Conf.csv"),
     destname=paste0(result_dir,"TempMigrationStats",ifelse(filter_dummy==1,paste0("_",filter_name),""),"/",estimate_type,"_",ifelse(filter_dummy==1,filter_name,"FullData"),"_",T_meso_min,"days_ByOD",timeUnit_type,"_",eventIdent_conf,"Conf.csv.gz"),
     overwrite=T)
