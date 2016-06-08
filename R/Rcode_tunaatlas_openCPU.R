plotQuantitiesByTonnes <- function(species=c(), start=1946, end=2014)  {
  library (DBI)
  library ("RPostgreSQL")
  library(rCharts)
  library(dplyr)
  library(jsonlite)

  speciesList <- ""
  for (sp in species) {
    speciesList <- paste0(speciesList, "'", sp, "'", ',')
  }
  speciesList <- substr(speciesList, 1, nchar(speciesList)-1)

  whereConditions <- ""
  whereConditions <- paste0(whereConditions," year <= ", end, " ")
  whereConditions <- paste0(whereConditions," AND ", " year >= ", start, " ")

  if (speciesList != "") {
    whereConditions <- paste0(whereConditions," AND ", " species.codesource_species IN (", speciesList, ") ")
  }
  whereConditions <- paste0(whereConditions," AND ", " id_catchunit IN (1,3) ")

  query <- "SELECT
  english_name_ocean as ASD,
  year as seasonYear,
  month as SeasonMonthNr,
  month || ' - ' || month_name as SeasonMonth,
  month_name as MonthNm,
  codesource_gear as GearCode,
  english_name_gear as Gear,
  species.codesource_species as SpeciesCode,
  scientific_name as ScientificName,

  family as ScientificFamilyName,
  v_catch as CatchWeight,
  scientific_name || ' - ' || species.codesource_species as Species,
  species.codesource_species as TargetSpeciesCode,
  codesource_flag as CountryCode,
  english_name_flag as Country

  from tunaatlas.total_catches tc

  INNER JOIN area.area USING (id_area)
  INNER JOIN time.time USING (id_time)
  INNER JOIN gear.gear_labels ON (tc.id_geargroup_standard=gear_labels.id_gear)
  INNER JOIN flag.flag_labels ON (tc.id_flag_standard=flag_labels.id_flag)
  INNER JOIN species.species_labels ON (tc.id_species_standard=species_labels.id_species)
  INNER JOIN species.species ON (tc.id_species_standard=species.id_species)
  INNER JOIN species.species_asfis ON (species.codesource_species=species_asfis.x3a_code)
  INNER JOIN area.rfmos_convention_areas_fao ON (rfmos_convention_areas_fao.id_origin_institution=tc.id_ocean)
  WHERE "

  query <- paste0(query, whereConditions)

  drv <- dbDriver("PostgreSQL")
  con_sardara <- dbConnect(drv, user = "invsardara",password="fle087",dbname="sardara_world",host ="db-tuna.d4science.org",port=5432)

  tuna <- dbGetQuery(con_sardara, query)

  colnames(tuna)<-c("ASD","SeasonYear","SeasonMonthNr","SeasonMonth","MonthNm","GearCode","Gear","TargetSpeciesCode","ScientificName","ScientificFamilyName","CatchWeightT","Species","SpeciesCode","CountryCode","Country")

  myData <- tuna

  aggr0 <- aggregate(myData$CatchWeightT, by=list(SeasonYear=myData$SeasonYear, Country=myData$Country), FUN=sum)
  aggr01 <- transform(aggr0, SeasonYear = as.character(SeasonYear), CatchWeightT = as.numeric(x))
  aggr01$x <- NULL
  aggr02 <- filter(aggr01, SeasonYear >= start, SeasonYear <= end)
  aggr03 <- aggregate(aggr02$CatchWeightT, by=list(Country=aggr02$Country), FUN=sum)
  aggr04 <- transform(aggr03, CatchWeightT = as.numeric(x))
  aggr04$x <- NULL
  m1 <- mPlot(x = "Country", y = "CatchWeightT", data = aggr04, stacked = "TRUE", xLabelAngle = 85)
  m1$save('output.html', standalone = TRUE)
  return (toJSON(aggr04))
}
