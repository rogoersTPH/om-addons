.datatable.aware <- TRUE ## see: https://cran.r-project.org/web/packages/data.table/vignettes/datatable-importing.html


##' @title List of measures needed to calculate epidemiological indicators
##' @keywords internal
##' @export
measuresNeededForPostprocessing_old <- function() {
  measuresNeededForPostprocessing_old <- list(
    prevalence = c("nPatent", "nHost"),
    incidence = c("nUncomp", "nSevere", "nHost"),
    tUncomp = c("nTreatments1", "nTreatments2"),
    tSevere = c("nTreatments3"),
    nHosp = c("nHospitalDeaths", "nHospitalRecovs", "nHospitalSeqs"),
    edeath = c("expectedDirectDeaths", "expectedIndirectDeaths", "nHost"),
    edirdeath = c("expectedDirectDeaths", "nHost"),
    ddeath = c("nIndDeaths", "nDirDeaths", "nHost")
  )
  ## We also add all output measures as they are
  dict <- omOutputDict_old()
  measuresNeededForPostprocessing_old <- c(
    measuresNeededForPostprocessing_old,
    stats::setNames(
      dict$measure_name,
      dict$measure_name
    )
  )
  return(measuresNeededForPostprocessing_old)
}


##' @title Open Malaria output dictionary
##' @description A dictionary which provides translations for the following
##'   outputs of Open Malaria:
##'
##'   - Survey measure numbers to names
##'
##'   - Whether measures are summed up between survey dates (incident = TRUE) or
##'   represent prevalent characteristics (incident = FALSE)
##'
##'   - An identifier for the 'third dimension' column. This can be 'age_group',
##' 'vector_species', 'drug_id' or NA
##'
##' See: https://github.com/SwissTPH/openmalaria/wiki/MonitoringOptions
##' To parse measure names from openMalaria v44.0 source code:
##' \preformatted{
##'   grep -oP '[\K[^\]]+' model/mon/OutputMeasures.h > tmp1.txt ;
##'   grep 'OutMeasure::' model/mon/OutputMeasures.h | cut -d, -f 2 > tmp2.txt
##'   cat tmp2.txt | while read line;do grep -r "mon::$line" model/Host model/interventions/ model/Clinical model/Transmission model/PkPd | grep -Po 'report\K[^(]' | paste -s -d, - >> tmp3.txt;done
##'   paste tmp.txt > surveyMeasureTable.txt
##'   rm tmp*.txt
##' }

##' @export
omOutputDict_old <- function() {
  dict <- data.table::data.table(
    measure_index = as.integer(c(
      0, 1, 2, 3, 4, 5, 6, 7, 8,
      10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
      20, 21, 22, 23, 24, 25, 26, 27,
      30, 31, 32, 33, 34, 35, 36, 39,
      40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
      50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
      60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
      70, 71, 72, 73, 74, 75, 76, 77, 78, 79
    )),
    measure_name = c(
      ## 0 - 8
      "nHost", "nInfect", "nExpectd", "nPatent", "sumLogPyrogenThres",
      "sumlogDens", "totalInfs", "nTransmit", "totalPatentInf",

      ## 10s
      "sumPyrogenThresh", "nTreatments1", "nTreatments2", "nTreatments3",
      "nUncomp", "nSevere", "nSeq", "nHospitalDeaths", "nIndDeaths",
      "nDirDeaths",

      ## 20s
      "nEPIVaccinations", "allCauseIMR", "nMassVaccinations", "nHospitalRecovs",
      "nHospitalSeqs", "nIPTDoses", "annAvgK", "nNMFever",

      ## 30s
      "innoculationsPerAgeGroup", "Vector_Nv0", "Vector_Nv", "Vector_Ov",
      "Vector_Sv", "inputEIR", "simulatedEIR", "Clinical_RDTs",

      ## 40s
      "Clinical_DrugUsage", "Clinical_FirstDayDeaths",
      "Clinical_HospitalFirstDayDeaths", "nNewInfections", "nMassITNs",
      "nEPI_ITNs", "nMassIRS", "nMassVA", "Clinical_Microscopy",
      "Clinical_DrugUsageIV",

      ## 50s
      "nAddedToCohort", "nRemovedFromCohort", "nMDAs", "nNmfDeaths",
      "nAntibioticTreatments", "nMassScreenings", "nMassGVI", "nCtsIRS",
      "nCtsGVI", "nCtsMDA",

      ## 60s
      "nCtsScreenings", "nSubPopRemovalTooOld", "nSubPopRemovalFirstEvent",
      "nLiverStageTreatments", "nTreatDiagnostics", "nMassRecruitOnly",
      "nCtsRecruitOnly", "nTreatDeployments", "sumAge", "nInfectByGenotype",

      ## 70s
      "nPatentByGenotype", "logDensByGenotype", "nHostDrugConcNonZero",
      "sumLogDrugConcNonZero", "expectedDirectDeaths", "expectedHospitalDeaths",
      "expectedIndirectDeaths", "expectedSequelae", "expectedSevere",
      "innoculationsPerVector"
    ),
    incident = c(
      ## 0 - 8
      FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE,

      ## 10s
      FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE,

      ## 20s
      TRUE, NA, TRUE, TRUE, TRUE, NA, FALSE, TRUE,

      ## 30s
      FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, NA,

      ## 40s
      NA, TRUE, TRUE, NA, TRUE, TRUE, TRUE, NA, NA, NA,

      ## 50s
      NA, NA, TRUE, TRUE, NA, TRUE, TRUE, TRUE, TRUE, TRUE,

      ## 60s
      TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, NA, FALSE, FALSE,

      ## 70s
      TRUE, FALSE, TRUE, NA, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE
    ),
    third_dimension = c(
      ## 0 - 8
      "age_group", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", NA, "age_group",

      ## 10s
      "age_group", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", "age_group", "age_group", "age_group",

      ## 20s
      "age_group", NA, "age_group", "age_group", "age_group", "age_group", NA,
      "age_group",

      ## 30s
      "age_group", "vector_species", "vector_species", "vector_species",
      "vector_species", NA, NA, NA,

      ## 40s
      "drug_id", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", "age_group", NA, "drug_id",

      ## 50s
      "age_group", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", "age_group", "age_group", "age_group",

      ## 60s
      "age_group", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", "age_group", "age_group", "age_group",

      ## 70s
      "age_group", "age_group", "age_group", "age_group", "age_group",
      "age_group", "age_group", "age_group", "age_group", NA
    )
  )
  return(dict)
}




##' @title Calculate epidemiological indicators from stacked raw data
##' @param dbName Name of sqlite data base file of output results, if null
##' search of sqlite file in rootDir
##' @param experiment_id Number of experiment id to process
##' @param scenario_id Numeric vector of scenario ids to process, if NULL then process all scenarios within given experiment
##' @param metadataFeatures Character vector of metadata features to keep, if null keep all
##' @param indicators Character vector of epidemiological indicators to calculate
##' @param customIndicatorFormula Character vector to parse and evaluate for custom indicators
##' @param keepMeasuresForIndicators List of indicators whose measures you want to keep in the function output, if null keep none
##' @param aggregateByAgeGroup Character vector of age groups (e.g. "2-10" for the
##' right-open interval) to be
##' used for aggregation, if NULL, then age groups from surveys will be used
##' @param aggregateByDate List (named "month", "year", "date") list(month="month",year="year",date=as.Date(c("2010-04-06","2012-04-06"))) to
##' be used for aggregation, if NULL, then survey dates will be used, for month, aggregated survey date is set to middle of month
##' @param wideFormat Logical vector, if TRUE than output in wide format
##' @param timeHorizon Character vector of with start date and end date of
##' time horizon to be processed, by default start on 2000-01-01
##' @export
calculateEpidemiologicalIndicators_old <- function(dbName = NULL,
                                                   experiment_id = NULL,
                                                   scenario_id = NULL,
                                                   metadataFeatures = NULL,
                                                   indicators = c("incidence", "prevalence", "simulatedEIR"),
                                                   customIndicatorFormula = NULL,
                                                   keepMeasuresForIndicators = NULL,
                                                   aggregateByAgeGroup = NULL,
                                                   aggregateByDate = NULL,
                                                   wideFormat = TRUE,
                                                   timeHorizon = c("2000-01-01", "2100-01-01")) {
  ## Read stacked survey output, change date format and filter by timeHorizon
  if (is.null(dbName)) {
    dbName <- dir(path = openMalariaUtilities::getCache("rootDir"), pattern = "\\.sqlite", full.names = F)
    ## TODO the location of the .sqlite file should be in the cache after readResults
    dbName <- gsub(".sqlite", "", dbName)
  }
  ## Open connection to database
  openMalariaOutput <- DBI::dbConnect(RSQLite::SQLite(), paste0(dbName, ".sqlite"))
  ## TODO in depth sanity checks for database before starting to process
  if (!all(c(
    "experiments", "placeholders", "results", "scenarios",
    "scenarios_metadata"
  ) %in% DBI::dbListTables(openMalariaOutput))) {
    stop(paste0(
      "The database ", dbName, " needs tables ",
      "experiments, placeholders, results, scenarios"
    ))
  }

  ## Read scenarios_metadata table
  message(paste0("Reading scenarios_metadata table from ", dbName))
  scenarios_metadata <- data.table::as.data.table(DBI::dbReadTable(openMalariaOutput, "scenarios_metadata"))
  scenarios_metadata <- data.table::dcast(scenarios_metadata,
    experiment_id + scenario_id ~ key_var,
    value.var = "value"
  )

  ## Reading results table, filter by experiment_id and scenario_id
  ## TODO more advanced filtering, sql query takes quite some time, parallelize?
  message(paste0("Reading results table from ", dbName))
  dbName <- gsub(".sqlite", "", dbName)
  if (!is.null(experiment_id) & !is.null(scenario_id)) {
    # results <- DBI::dbGetQuery(openMalariaOutput, paste0("SELECT experiment_id, scenario_id, survey_date, third_dimension, measure, value FROM results WHERE experiment_id IN (1,2)
    results <- DBI::dbGetQuery(openMalariaOutput, paste0("SELECT experiment_id, scenario_id, survey_date, third_dimension, measure, value FROM results WHERE experiment_id = ", experiment_id, " AND ( ", paste0("scenario_id = ", scenario_id, collapse = " OR "), " )"))
  } else {
    results <- DBI::dbReadTable(openMalariaOutput, "results")
  }

  ## Check measures in results table and translate to human readable names
  results <- data.table::as.data.table(results)
  ## TODO update omOutputDict_old in openMalariaUtilities, then replace omOutputDict_old by openMalariaUtilities::omOutputDict_old in the code
  omOutputDict_old <- data.table::as.data.table(omOutputDict_old())
  measureInResults <- omOutputDict_old[measure_name %in% unique(results$measure), ]
  data.table::setnames(measureInResults, c("measure_name", "third_dimension"), c("measure", "third_dimension_name"))
  message("Your results have the following measure names: ")
  message(paste0(measureInResults$measure, sep = "\n"))
  results <- results[measureInResults[, -c("measure_index")], on = "measure", ]

  ## Check if we have all measures required for indicators
  ## TODO also for customIndicatorFormula
  measuresNeededForPostprocessing_old <- measuresNeededForPostprocessing_old()
  if ("all" %in% indicators || is.null(indicators)) {
    indicators <- names(measuresNeededForPostprocessing_old)
  }
  requiredMeasures <- unique(unlist(measuresNeededForPostprocessing_old[indicators]))
  if (any(requiredMeasures %in% results$measure == FALSE)) {
    stop(paste0("To calculate chosen indicators, you need measures ",
      paste(unique(requiredMeasures[requiredMeasures %in% results$measure == FALSE]), collapse = ", "), " in your output files.",
      collapse = ""
    ))
  }
  results <- results[measure %in% requiredMeasures]
  results <- results[, survey_date := as.Date(survey_date)]
  results <- results[survey_date >= timeHorizon[1] & survey_date <= timeHorizon[2]]

  ## Reading thirdDimension table from cache and map ids to human-readable names
  message("Reading thirdDimension table from experiment cache and joining to results table.")
  thirdDimension <- unique(openMalariaUtilities::getCache("thirdDimension"))
  data.table::setnames(thirdDimension, c("id", "value", "name"), c("third_dimension_name", "third_dimension", "third_dimension_level"))
  thirdDimension[, third_dimension := as.character(thirdDimension$third_dimension)]
  results <- merge(results, thirdDimension, all.x = T, by = c("third_dimension_name", "third_dimension"))
  results[, third_dimension := NULL]

  ## Read placeholder table
  message(paste0("Reading placeholders table from ", dbName))
  placeholders <- DBI::dbReadTable(openMalariaOutput, "placeholders")
  placeholders <- data.table::as.data.table(placeholders)
  placeholders <- data.table::dcast(placeholders,
    experiment_id + scenario_id ~ placeholder,
    value.var = "value"
  )

  ## Merge into metadata and keep selected metadata features
  metadata <- merge(scenarios_metadata,
    placeholders,
    by = c("experiment_id", "scenario_id")
  )

  if (!is.null(metadataFeatures)) {
    metadataFeatures <- unique(unlist(lapply(metadataFeatures, function(x) colnames(metadata)[grepl(x, colnames(metadata))])))
    if (length(metadataFeatures) == 0) {
      message("None of your metadata features or patterns where found.
              Only default features (experiment_id,scenario_id,seed,pop) will be added.")
    }
    metadataFeatures <- unique(c(metadataFeatures, c("experiment_id", "scenario_id", "seed", "pop")))
    metadata <- metadata[, ..metadataFeatures]
  }

  ## Specify and/or add age groups, aggregateByAgeGroup<-c("0-5","2-10"), we assume for all measures the same age stratification in reporting
  resultsAge <- results[third_dimension_name == "age_group", ]
  if (nrow(resultsAge) > 0) {
    if (!is.null(aggregateByAgeGroup)) {
      aggregateAge_list <- unlist(lapply(aggregateByAgeGroup, function(x) as.numeric(strsplit(unique(x), "-")[[1]])))
      age_group_numbers <- unlist(lapply(as.character(thirdDimension$third_dimension_level), function(x) as.numeric(strsplit(unique(x), "-")[[1]])))
      if (!all(unlist(aggregateAge_list) %in% age_group_numbers)) {
        stop("To use aggregateByAgeGroup, you need all of its age groups present in the metadata!")
      }
      for (intrvl in aggregateByAgeGroup)
      {
        age_group_aggr_numbers <- as.numeric(strsplit(intrvl, "-")[[1]])
        age_groups_inbetween <- age_group_numbers[age_group_numbers >= age_group_aggr_numbers[1] & age_group_numbers <= age_group_aggr_numbers[2]]
        age_groups_inbetween_intervals <- paste(head(age_groups_inbetween, -1), tail(age_groups_inbetween, -1), sep = "-")
        age_group_aggr_table <- data.table::as.data.table(
          data.frame(
            third_dimension_level = age_groups_inbetween_intervals,
            age_group_aggregated = intrvl
          ),
          key = "third_dimension_level"
        )
        results1 <- resultsAge[third_dimension_level %in% age_groups_inbetween_intervals][age_group_aggr_table, on = "third_dimension_level"]
        results1 <- results1[, third_dimension_level := NULL]
        data.table::setnames(results1, "age_group_aggregated", "third_dimension_level")
        results1 <- results1[, .(value = sum(value)), .(experiment_id, scenario_id, incident, measure, survey_date, third_dimension_name)]
        results1[, third_dimension_level := intrvl]
        resultsAge <- rbind(
          resultsAge,
          results1
        )
      }
      resultsAge <- resultsAge[third_dimension_level %in% aggregateByAgeGroup]
      message(paste0("Aggregation by age for age classes ", paste0(aggregateByAgeGroup, collapse = ", ")))
    }
  }
  results <- rbind(
    resultsAge,
    results[third_dimension_name != "age_group" | is.na(third_dimension_name), ]
  )

  ## Aggregation by date
  ## Need to distinguish aggregation of incident=T (sum) and incident=F (mean) survey measures, aggregateByDate<-list(month="month",year="year",date=as.Date(c("2010-04-06","2012-04-06")))
  ## By default, monthly aggregation is done on the 15th of each month, yearly aggregation is done over the first to the last survey date in each year
  ## TODO Need to make sure that aggregation is in line with surveys (e.g. three months survey data should not allow to have monthly aggregation)
  results_aggregateDate <- list()
  if (!is.null(aggregateByDate)) {
    if ("year" %in% names(aggregateByDate)) {
      results[, aggregation_year := format(survey_date, "%Y")]
      results_year <- rbind(
        results[
          measure %in% omOutputDict_old()[incident == T, measure_name],
          .(value = sum(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_year, third_dimension_name)
        ],
        results[
          measure %in% omOutputDict_old()[incident == F, measure_name],
          .(value = mean(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_year, third_dimension_name)
        ]
      )
      data.table::setnames(results_year, "aggregation_year", "date")
      results_year[, date := as.Date(date, format = "%Y")]
      results_year[, dateAggregation := "year"]
      results_aggregateDate$year <- results_year
    }
    if ("month" %in% names(aggregateByDate)) {
      results[, aggregation_month := format(survey_date, "%Y-%m")]
      results_month <- rbind(
        results[
          measure %in% omOutputDict_old()[incident == T, measure_name],
          .(value = sum(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_month, third_dimension_name)
        ],
        results[
          measure %in% omOutputDict_old()[incident == F, measure_name],
          .(value = mean(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_month, third_dimension_name)
        ]
      )
      data.table::setnames(results_month, "aggregation_month", "date")
      results_month[, date := as.Date(paste0(date, "-15"), format = "%Y-%m-%d")]
      results_month[, dateAggregation := "month"]
      results_aggregateDate$month <- results_month
    }
    if ("date" %in% names(aggregateByDate)) {
      if (range(sort(aggregateByDate[["date"]]))[1] < timeHorizon[1] | range(sort(aggregateByDate[["date"]]))[2] > timeHorizon[2]) {
        stop("Aggregation dates are not within the defined time horizon.")
      }
      results <- data.table::as.data.table(data.frame(
        survey_date = aggregateByDate[["date"]],
        aggregation_date = aggregateByDate[["date"]]
      ))[results, roll = "nearest", on = "survey_date"]
      results_dates <- rbind(
        results[
          measure %in% omOutputDict_old()[incident == T, measure_name],
          .(value = sum(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_date, third_dimension_name)
        ],
        results[
          measure %in% omOutputDict_old()[incident == F, measure_name],
          .(value = mean(value)),
          .(third_dimension_level, experiment_id, scenario_id, measure, aggregation_date, third_dimension_name)
        ]
      )
      data.table::setnames(results_dates, "aggregation_date", "date")
      results_dates[, dateAggregation := "date"]
      results_aggregateDate$date <- results_dates
    }
    message(paste0("Aggregation by date: ", paste0(unlist(lapply(aggregateByDate, as.character)), collapse = ", ")))
  }
  results_survey <- rbind(
    results[
      measure %in% omOutputDict_old()[incident == T, measure_name],
      .(value = sum(value)),
      .(third_dimension_level, experiment_id, scenario_id, measure, survey_date, third_dimension_name)
    ],
    results[
      measure %in% omOutputDict_old()[incident == F, measure_name],
      .(value = mean(value)),
      .(third_dimension_level, experiment_id, scenario_id, measure, survey_date, third_dimension_name)
    ]
  )
  results_survey[, dateAggregation := "survey"]
  data.table::setnames(results_survey, "survey_date", "date")
  results_aggregateDate$survey <- results_survey
  results_aggregated <- do.call("rbind", results_aggregateDate)
  rm(results)
  rm(results_aggregateDate)
  ## Switch survey output to wide format and merge with metadata
  results_aggregated <- merge(
    data.table::dcast(results_aggregated,
      experiment_id + scenario_id + third_dimension_level + third_dimension_name + date + dateAggregation ~ measure,
      value.var = "value"
    ),
    metadata,
    by = c("experiment_id", "scenario_id")
  )

  ## Start calculations
  message(paste0("Calculating epidemiological indicators: ", paste(indicators, collapse = ", ")))

  if ("incidence" %in% indicators) {
    results_aggregated[, incidenceRate := (nUncomp + nSevere) / nHost]
    results_aggregated[, incidenceRatePerThousand := incidenceRate * 1000]
  }

  if ("prevalence" %in% indicators) {
    results_aggregated[, prevalenceRate := nPatent / nHost, ]
  }

  if ("tUncomp" %in% indicators) {
    results_aggregated[, tUncomp := nTreatments1 + nTreatments2]
  }

  if ("tSevere" %in% indicators) {
    results_aggregated[, tSevere := nTreatments3]
  }

  if ("nHosp" %in% indicators) {
    results_aggregated[, nHosp := nHospitalDeaths + nHospitalRecovs + nHospitalSeqs]
  }

  if ("edeath" %in% indicators) {
    results_aggregated[, edeathRatePerHundredThousand := (expectedDirectDeaths + expectedIndirectDeaths) / nHost * 1e5]
    results_aggregated[, edeath := expectedDirectDeaths + expectedIndirectDeaths]
  }

  if ("edirdeath" %in% indicators) {
    results_aggregated[, edirdeathRatePerHundredThousand := expectedDirectDeaths / nHost * 1e5]
    results_aggregated[, edirdeath := expectedDirectDeaths]
  }

  if ("ddeath" %in% indicators) {
    results_aggregated[, ddeath := nIndDeaths + nDirDeaths]
    results_aggregated[, ddeathRatePerHundredThousand := (nIndDeaths + nDirDeaths) / nHost * 1e5]
  }
  ## We add custom indicators
  if (!is.null(customIndicatorFormula)) {
    message(paste0("Calculating custom epidemiological indicators: ", paste(names(customIndicatorFormula), collapse = ", ")))
    for (id in names(customIndicatorFormula)) {
      results_aggregated[, (id) := eval(parse(text = customIndicatorFormula[[id]]))]
    }
  }


  if (is.null(keepMeasuresForIndicators)) {
    keepMeasures <- NULL
    keepMeasures <- c(keepMeasures, indicators[indicators %in% omOutputDict_old()[, measure_name]])
  } else {
    keepMeasures <- measuresNeededForPostprocessing_old[keepMeasuresForIndicators] %>%
      unlist() %>%
      unique()
  }
  removeMeasures <- setdiff(requiredMeasures, keepMeasures)

  results_aggregated <- results_aggregated[, -..removeMeasures]
  results_aggregated[, date := as.character(date)]
  results_aggregated[, third_dimension_name := as.factor(third_dimension_name)]
  results_aggregated[, experiment_id := as.factor(experiment_id)]
  results_aggregated[, scenario_id := as.factor(scenario_id)]
  results_aggregated[, seed := as.factor(seed)]

  if (wideFormat == F) {
    results_aggregated <- data.table::melt(results_aggregated,
      measure.vars = colnames(results_aggregated)[apply(sapply(indicators, function(x) grepl(x, colnames(results_aggregated))), 1, any)]
    )
  }

  return(results_aggregated)
}




##' @title Wrapper function to do postprocessing
##' @param experimentDir Experiment directory, if null then use current working directory
##' @param dbName Name of the sqlite file, if null then use name of rootDir
##' @param readResults Logical, if TRUE extract data and overwrites sqlite file while creating data base from output files
##' @param experiment_id Number of experiment id to process
##' @param scenario_id Numeric vector of scenario ids to process, if NULL then process all scenarios within given experiment
##' @param batch_id Numeric, adds batch_id to processed data if NULL then nothing is added
##' @param metadataFeatures Character vector of metadata features (patterns are matched) to keep, if null keep all
##' @param indicators Character vector of epidemiological indicators to calculate
##' @param customIndicatorFormula Character vector to parse and evaluate for custom indicators
##' @param keepMeasuresForIndicators Character vector of indicators whose measures we want to keep
##' @param aggregateByAgeGroup Character vector of age groups (e.g. "2-10" for the
##' right-open interval) to be
##' used for aggregation, if NULL, then age groups from surveys will be used
##' @param aggregateByDate List (named "month", "year", "date") list(month="month",year="year",date=as.Date(c("2010-04-06","2012-04-06"))) to
##' be used for aggregation, if NULL, then survey dates will be used, for month, aggregated survey date is set to middle of month
##' @param wideFormat Logical vector, if TRUE than output in wide format
##' @param timeHorizon Character vector of start date and end date of
##' time horizon to be processed, by default start on 2000-01-01
##' @param outputFilename Character vector file name for processed data (.rds and .csv allowed), if NULL then write to epidemiologicalIndicators.rds in rootDir
##' @export
postprocess <- function(experimentDir = NULL,
                        dbName = NULL,
                        experiment_id = NULL,
                        scenario_id = NULL,
                        batch_id = NULL,
                        readResults = F,
                        metadataFeatures = NULL,
                        indicators = c("incidence", "prevalence"),
                        customIndicatorFormula = NULL,
                        keepMeasuresForIndicators = NULL,
                        aggregateByAgeGroup = NULL,
                        aggregateByDate = NULL,
                        wideFormat = T,
                        timeHorizon = c("2000-01-01", "2100-01-01"),
                        outputFilename = NULL) {
  ## TODO parallelize with ncores


  ## Load necessary information
  if (is.null(experimentDir)) {
    experimentDir <- getwd()
  }
  # openMalariaUtilities::syncCache(path=experimentDir)

  if (is.null(dbName)) {
    dbName <- basename(openMalariaUtilities::getCache("rootDir"))
  }
  if (readResults) {
    openMalariaUtilities::readResults(experimentDir, dbName = dbName, replace = readResults)
  }

  ## Postprocess output files
  epidemiologicalIndicators <- calculateEpidemiologicalIndicators_old(
    dbName = file.path(openMalariaUtilities::getCache(x = "rootDir"), dbName),
    experiment_id = experiment_id,
    scenario_id = scenario_id,
    metadataFeatures = metadataFeatures,
    indicators = indicators,
    customIndicatorFormula = customIndicatorFormula,
    keepMeasuresForIndicators = keepMeasuresForIndicators,
    aggregateByAgeGroup = aggregateByAgeGroup,
    aggregateByDate = aggregateByDate,
    wideFormat = wideFormat,
    timeHorizon = timeHorizon
  )
  if (is.null(outputFilename)) {
    outputFilename <- file.path(openMalariaUtilities::getCache("rootDir"), "epidemiologicalIndicators.rds")
  }
  if (!is.null(batch_id)) {
    epidemiologicalIndicators$batch_id <- batch_id
  }
  message(paste0("Writing processed data to ", outputFilename))

  extension <- tools::file_ext(outputFilename)
  if (extension == "rds" | extension == "RDS") {
    saveRDS(epidemiologicalIndicators, file = outputFilename)
  } else if (extension == "csv" | extension == "CSV") {
    readr::write_csv(epidemiologicalIndicators, file = outputFilename)
  } else if (is.null(extension)) {
    message(paste0("Writing epidemiologicalIndicators to cache."))
    putCache("epidemiologicalIndicators", epidemiologicalIndicators)
  } else {
    message("Only .csv and .rds file extensions are supported.")
  }
}
