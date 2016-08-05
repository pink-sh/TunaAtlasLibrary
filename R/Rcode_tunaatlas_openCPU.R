plotQuantitiesInTonnesByCountry <- function(species=c(), start=1946, end=2014, chart="Bar")  {
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

  from tunaatlas.catches tc

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
  m1 <- mPlot(x = "Country", y = "CatchWeightT", type = chart, data = aggr04, stacked = "TRUE", xLabelAngle = 85)
  m1$save('output.html', standalone = TRUE)
  return (toJSON(aggr04))
}

getSpecies <- function() {
  library(jsonlite)
  library(plyr)
  library(RCurl)
  library (DBI)
  library ("RPostgreSQL")
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
  INNER JOIN gear.gear_labels ON (tc.id_geargroup_tunaatlas=gear_labels.id_gear)
  INNER JOIN flag.flag_labels ON (tc.id_flag_standard=flag_labels.id_flag)
  INNER JOIN species.species_labels ON (tc.id_species_standard=species_labels.id_species)
  INNER JOIN species.species ON (tc.id_species_standard=species.id_species)
  INNER JOIN species.species_asfis ON (species.codesource_species=species_asfis.x3a_code)
  INNER JOIN area.rfmos_convention_areas_fao ON (rfmos_convention_areas_fao.id_origin_institution=tc.id_ocean)
  WHERE year<=2014 AND id_catchunit IN (1,3) AND v_catch > 0"
  drv <- dbDriver("PostgreSQL")
  con_sardara <- dbConnect(drv, user = "invsardara",password="fle087",dbname="sardara_world",host ="db-tuna.d4science.org",port=5432)
  tuna <- dbGetQuery(con_sardara, query)
  colnames(tuna)<-c("ASD","SeasonYear","SeasonMonthNr","SeasonMonth","MonthNm","GearCode","Gear","TargetSpeciesCode","ScientificName","ScientificFamilyName","CatchWeightT","Species","SpeciesCode","CountryCode","Country")
  myData <- tuna
  plyed <- ddply(myData, c("ScientificName","SpeciesCode"), head, 1)
  ret <- data.frame("id" = plyed$SpeciesCode, "name" = plyed$ScientificName)
  ret <- ret[!apply(ret, 1, function(x) any(x=="")),]
  return (toJSON(ret))
}

plotQuantitiesInTonnes <- function(species=c(), polygons=c(), start=1946, end=2014, chart="Bar") {
  vector.is.empty <- function(x) return(length(x) ==0 )
  library (DBI)
  library ("RPostgreSQL")
  library(rCharts)
  library(dplyr)
  library(plyr)
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
  whereConditions <- paste0(whereConditions," AND ", " id_catchunit IN (1,3) AND v_catch > 0 ")
  
  if (!vector.is.empty(polygons)) {
    whereConditions <- paste0(whereConditions," AND ST_IsValid(AWG.geom) AND (")
    polygonList <- "";
    for (polygon in polygons) {
      polygonList <- paste0(polygonList, "ST_Intersects(ST_SetSRID(AWG.geom, 4326) ,ST_SetSRID(ST_GeomFromText('MULTIPOLYGON(((", polygon, ")))'), 4326)) OR ")
    }
    polygonList <- substr(polygonList,1,nchar(polygonList)-3)
    print (polygonList)
    whereConditions <- paste0(whereConditions, polygonList, ")")
  }
  
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
  english_name_flag as Country,
	ST_AsText(AWG.geom) as polygons
  
  from tunaatlas.catches tc
  
  INNER JOIN area.area USING (id_area)
  INNER JOIN time.time USING (id_time)
  INNER JOIN gear.gear_labels ON (tc.id_geargroup_tunaatlas=gear_labels.id_gear)
  INNER JOIN flag.flag_labels ON (tc.id_flag_standard=flag_labels.id_flag)
  INNER JOIN species.species_labels ON (tc.id_species_standard=species_labels.id_species)
  INNER JOIN species.species ON (tc.id_species_standard=species.id_species)
  INNER JOIN species.species_asfis ON (species.codesource_species=species_asfis.x3a_code)
  INNER JOIN area.rfmos_convention_areas_fao ON (rfmos_convention_areas_fao.id_origin_institution=tc.id_ocean)
  INNER JOIN area.areas_with_geom AWG ON (tc.id_area = AWG.id_area)
  WHERE "
  
  query <- paste0(query, whereConditions)
  
  #print(query)
  
  drv <- dbDriver("PostgreSQL")
  con_sardara <- dbConnect(drv, user = "invsardara",password="fle087",dbname="sardara_world",host ="db-tuna.d4science.org",port=5432)
  
  tuna <- dbGetQuery(con_sardara, query)
  dbDisconnect(con_sardara)
  
  if (is.data.frame(tuna) && nrow(tuna)==0) {
    return (toJSON(tuna))
  }

  poly <- count(data.frame(matrix(unlist(tuna$polygons), byrow=T)))
  names(poly)[1] <- "polygons"
  tuna <- subset(tuna, select = -c(polygons) )
  colnames(tuna)<-c("ASD","SeasonYear","SeasonMonthNr","SeasonMonth","MonthNm","GearCode","Gear","TargetSpeciesCode","ScientificName","ScientificFamilyName","CatchWeightT","Species","SpeciesCode","CountryCode","Country")
  
  #myCsv <- getURL(file)
  #myData <- read.csv(textConnection(myCsv))
  myData <- tuna
  if (length(species > 0)) {
    reduced <- filter(myData, SpeciesCode %in% species)
  } else {
    reduced <- myData
    species <- unique(reduced$SpeciesCode)
  }
  OUT <- data.frame("SeasonYear"=(start:end))
  for (sp in species) {
    aggr0 <- filter(reduced, SpeciesCode == sp)
    aggr01 <- aggregate(aggr0$CatchWeightT, by=list(SeasonYear=aggr0$SeasonYear, ScientificName=aggr0$ScientificName), FUN=sum)
    aggr02 <- transform(aggr01, SeasonYear = as.character(SeasonYear), CatchWeightT = as.numeric(x))
    aggr03 <- filter(aggr02, SeasonYear >= start, SeasonYear <= end)
    ifelse(lengths(aggr03, use.names = TRUE) == 0,next,1)
    aggr03$x <- NULL
    
    
    vector <- c()
    i = 1;
    scientificName <- ""
    apply(OUT, 1, function(row1) {
      yrOut = row1['SeasonYear']
      value <- 0
      apply(aggr03, 1, function(row2) {
        yrIn = row2['SeasonYear']
        
        scientificName <<- row2['ScientificName']
        if (!is.na(yrIn)) {
          if (yrOut == yrIn) {
            value <<- row2['CatchWeightT']
          }
        }
      })
      vector <<- c(vector, value)
      i <<- i + 1
    })
    OUT[[scientificName]] <- vector
  }
  OUT <- transform(OUT, SeasonYear = as.character(SeasonYear))
  namesOut <- c()
  for (name in names(OUT)) {
    if (name != 'SeasonYear') {
      OUT[, name] = as.double(OUT[, name])
      namesOut <- c(namesOut, name)
    }
  }
  m1 <- mPlot(x = "SeasonYear", y = namesOut, type = chart, data = OUT, stacked = "TRUE")
  m1$set(hoverCallback = "#! function(index, options, content) {
         var row = options.data[index];
         var tuples = [];
         var YEAR = '';
         for (var key in row) {
         if (key != 'SeasonYear') {
         tuples.push([key, parseInt(row[key])]);
         } else {
         year = row[key]
         }
         }
         tuples.sort(function(a, b) {return b[1] - a[1]});
         var ret = [];
         ret.push('<div style=\"color: red;\">' + year + '</div>');
         for (var i = 0; i < tuples.length; i++) {
         if (i > 10) break;
         ret.push(tuples[i][0].replace('.', ' ') + ':' + tuples[i][1]);
         }
         return ret.join('<br />'); } !#")
  m1$save('output.html', standalone = TRUE)
  return (toJSON(list(OUT, poly), pretty = TRUE))
}

getCatchForSpeciesOnTimeFrame <- function(species=c(), polygons=c(), start=1946, end=2014) {
  vector.is.empty <- function(x) return(length(x) ==0 )
  library (DBI)
  library ("RPostgreSQL")
  library(dplyr)
  library(plyr)
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
  whereConditions <- paste0(whereConditions," AND ", " id_catchunit IN (1,3) AND v_catch > 0 ")
  
  if (!vector.is.empty(polygons)) {
    whereConditions <- paste0(whereConditions," AND ST_IsValid(AWG.geom) AND (")
    polygonList <- "";
    for (polygon in polygons) {
      polygonList <- paste0(polygonList, "ST_Intersects(ST_SetSRID(AWG.geom, 4326) ,ST_SetSRID(ST_GeomFromText('MULTIPOLYGON(((", polygon, ")))'), 4326)) OR ")
    }
    polygonList <- substr(polygonList,1,nchar(polygonList)-3)
    print (polygonList)
    whereConditions <- paste0(whereConditions, polygonList, ")")
  }
  
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
  english_name_flag as Country,
  ST_AsText(AWG.geom) as polygons
  
  from tunaatlas.catches tc
  
  INNER JOIN area.area USING (id_area)
  INNER JOIN time.time USING (id_time)
  INNER JOIN gear.gear_labels ON (tc.id_geargroup_tunaatlas=gear_labels.id_gear)
  INNER JOIN flag.flag_labels ON (tc.id_flag_standard=flag_labels.id_flag)
  INNER JOIN species.species_labels ON (tc.id_species_standard=species_labels.id_species)
  INNER JOIN species.species ON (tc.id_species_standard=species.id_species)
  INNER JOIN species.species_asfis ON (species.codesource_species=species_asfis.x3a_code)
  INNER JOIN area.rfmos_convention_areas_fao ON (rfmos_convention_areas_fao.id_origin_institution=tc.id_ocean)
  INNER JOIN area.areas_with_geom AWG ON (tc.id_area = AWG.id_area)
  WHERE "
  
  query <- paste0(query, whereConditions)
  
  drv <- dbDriver("PostgreSQL")
  con_sardara <- dbConnect(drv, user = "invsardara",password="fle087",dbname="sardara_world",host ="db-tuna.d4science.org",port=5432)
  
  tuna <- dbGetQuery(con_sardara, query)
  dbDisconnect(con_sardara)
  
  colnames(tuna)<-c("ASD","SeasonYear","SeasonMonthNr","SeasonMonth","MonthNm","GearCode","Gear","TargetSpeciesCode","ScientificName","ScientificFamilyName","CatchWeightT","Species","SpeciesCode","CountryCode","Country", "Polygon")
  drops <- c("ASD","SeasonYear","SeasonMonthNr","SeasonMonth","MonthNm","GearCode","Gear","TargetSpeciesCode","ScientificName","ScientificFamilyName","Species","SpeciesCode","CountryCode","Country")
  
  yearsT <- sort(unique(tuna[['SeasonYear']], incomparables = FALSE))

  res <- list()

  for (yearT in yearsT) {
    t <- tuna[tuna$SeasonYear == yearT, ]
    t <- t[ , !(names(t) %in% drops)]
    t <- aggregate(t[,c("CatchWeightT")], by=list(t$Polygon), "sum")
    t <- t[with(t, order(-x, Group.1)), ]
    colnames(t)<-c("Polygon","CatchWeghtT")
    res[[toString(yearT)]] <- t
  }
  print(res)
  print(toJSON(res, pretty = FALSE))
}
