## See: https://cran.r-project.org/web/packages/data.table/vignettes/datatable-importing.html
.datatable.aware <- TRUE

##' @title Calculate epidemiological indicators from stacked raw data
##' @param df Input data frame
##' @param indicators Can be a character vector of epidemiological indicators to
##'   calculate or measures from OpenMalaris. The calculated indicators can be
##'   any of:
##'     prevalenceRate
##'     incidenceRate
##'     incidenceRatePerThousand
##'     tUncomp
##'     tSevere
##'     nHosp
##'     edeath
##'     edeathRatePerHundredThousand
##'     edirdeath
##'     edirdeathRatePerHundredThousand
##'     ddeath
##'     ddeathRatePerHundredThousand
##' @param aggregateByAgeGroup Character vector of age groups (e.g. "2-10" for
##'   the right-open interval) to be used for aggregation, if NULL, then age
##'   groups from surveys will be used
##' @param aggregateByDate If NULL, keep original surveys. Otherwise any
##'   combination of:
##'
##'        - "year" aggregate by year (date will be changed to 31-12 of the
##'          corresponding year).
##'
##'        - "month" aggregate by month (date will be changed to the 15th of a
##'          month).
##'
##'        - "survey" keep survey dates.
##' @param timeHorizon Character vector of with start date and end date of time
##'   horizon to be processed, e.g. c("2000-01-01", "2100-01-01")
##' @param use.gc If TRUE, run garbage collection during operation to keep
##'   memory consumption low. Useful if computational ressources are tight.
##' @importFrom data.table ':=' .SD
##' @export
CalcEpiOutputs <- function(df, indicators = c("incidenceRate", "prevalenceRate"),
                           aggregateByAgeGroup = NULL, aggregateByDate = NULL,
                           timeHorizon = NULL, use.gc = FALSE) {
  ## Make sure input is a data.table
  df <- data.table::as.data.table(df)

  ## Check if the required measures are available
  reqMeasures <- list(
    prevalenceRate = c("nPatent", "nHost"),
    incidenceRate = c("nUncomp", "nSevere", "nHost"),
    incidenceRatePerThousand = c("nUncomp", "nSevere", "nHost"),
    tUncomp = c("nTreatments1", "nTreatments2"),
    tSevere = c("nTreatments3"),
    nHosp = c("nHospitalDeaths", "nHospitalRecovs", "nHospitalSeqs"),
    edeath = c("expectedDirectDeaths", "expectedIndirectDeaths", "nHost"),
    edeathRatePerHundredThousand = c(
      "expectedDirectDeaths",
      "expectedIndirectDeaths", "nHost"
    ),
    edirdeath = c("expectedDirectDeaths", "nHost"),
    edirdeathRatePerHundredThousand = c("expectedDirectDeaths", "nHost"),
    ddeath = c("nIndDeaths", "nDirDeaths", "nHost"),
    ddeathRatePerHundredThousand = c("nIndDeaths", "nDirDeaths", "nHost")
  )

  ## These columns are not measures and should be ignored
  ## TODO Add cohorts, genotypypes, etc.
  fixedCols <- c(
    "scenario_id", "date_aggregation", "date", "age_group",
    "cohort"
  )
  indicators <- indicators[!indicators %in% fixedCols]

  ## Select needed measures for calculations and check if they are in the input.
  ## If not, stop.
  missingMeasures <- unlist(
    reqMeasures[indicators]
  )[!unlist(reqMeasures[indicators]) %in% unique(df[["measure"]])]
  if (length(missingMeasures > 0)) {
    stop(paste("The following measures are missing in your OM output:",
      paste0(unique(missingMeasures), collapse = "\n"),
      "Aborting.",
      sep = "\n"
    ))
  }

  ## Collect measures for calculation
  neededMeasures <- unique(unlist(reqMeasures[indicators]))
  ## Add other selected measures
  neededMeasures <- union(neededMeasures, indicators)
  ## We only keep the output columns as defined in indicators.
  dropCols <- neededMeasures[!neededMeasures %in% indicators]

  ## Narrow table to selected dates and measures
  df <- df[measure %in% neededMeasures]
  df <- df[, survey_date := as.Date(survey_date)]
  if (!is.null(timeHorizon)) {
    df <- df[survey_date >= timeHorizon[1] & survey_date <= timeHorizon[2]]
  }

  ## Split and rename third dimension column and aggregate
  ##
  ## TODO Implement cohorts, which requires the creation of an extra column.
  ##      | third_dimension |  ->  | cohort | age_roup |
  ##      |     AB:0-1      |  ->  |   AB   |    0-1   |
  ##
  ## Don't forget the "none" cohort, which should contain individuals not in any
  ## cohort.
  ## Also take special care with the "none"/0 age group!
  data.table::setnames(df, old = "third_dimension", new = "age_group")

  ## NOTE This needs to be extended if we also use genotypes, etc.


  ## NOTE All of the following aggregations are bottlenecks. So make sure that
  ##      these are as fast as possible. Our best bet is to make sure that
  ##      data.table's GForce optimizes the calls. These are fucking fast.
  ##      Verify and benchmark with options(datatable.verbose = TRUE).

  ## Age aggregation
  if (!is.null(aggregateByAgeGroup)) {
    groups <- aggregateByAgeGroup
    ## Translate "All" to a huge age range. Nobody should be older than 200,
    ## right?
    groups <- data.table::data.table(x = replace(groups, groups == "All", "0-200"))
    ages <- unique(df[["age_group"]])
    ## Remove "none" group, if present. This suppresses an annoying warning in
    ## the following age group selection.
    ages <- ages[!ages %in% "none"]
    tmp <- data.table::data.table(x = ages)

    ## Split the age group string (e.g. "0-1") into lo and hi columns (e.g. 1 0)
    groups <- groups[, c("lo", "hi") := data.table::tstrsplit(
      x, "-",
      fixed = TRUE
    )][, c("lo", "hi") := list(as.numeric(lo), as.numeric(hi))]

    tmp <- tmp[, c("lo", "hi") := data.table::tstrsplit(
      x, "-",
      fixed = TRUE
    )][
      ,
      c("lo", "hi") := list(as.numeric(lo), as.numeric(hi))
    ]

    ## Now check which monitoring age groups are in the range of the requested
    ## age groups. We will use this information later to sum up the values.
    selAges <- list()

    for (i in seq_len(nrow(groups))) {
      nn <- unlist(strsplit(as.character(ages[which(tmp[["lo"]] >= groups[[i, "lo"]] & tmp[["hi"]] <= groups[[i, "hi"]])]), "-", fixed = TRUE))
      nn <- ifelse(length(nn) > 0, paste0(nn[1], "-", nn[length(nn)]), NA)
      selAges[[groups[[i, "x"]]]][["groups"]] <- as.character(ages[which(tmp[["lo"]] >= groups[[i, "lo"]] & tmp[["hi"]] <= groups[[i, "hi"]])])
      selAges[[groups[[i, "x"]]]][["adj_name"]] <- nn
    }
    ## Change 0-200 back to All to avoid confusion
    names(selAges)[names(selAges) == "0-200"] <- "All"
    ## Also add back the "none" group
    selAges <- c(selAges, list(none = list(
      groups = "none",
      adj_name = NA
    )))

    ## Now we sum up the values for the desired age groups
    df <- data.table::rbindlist(
      lapply(seq_along(selAges), function(i, list) {
        if (length(list[[i]][["groups"]]) == 0) {
          stop(paste0(
            "Data for age group ", names(list[i]),
            " could not retrieved. Check boundaries.\n"
          ))
        }

        if (!is.na(list[[i]][["adj_name"]]) & list[[i]][["adj_name"]] != names(list[i])) {
          warning(
            paste0(
              "Requested age group ", names(list[i]),
              " does not fit monitored boundaries and was adjusted to ",
              list[[i]][["adj_name"]], "\n"
            )
          )
        }
        df[age_group %in% list[[i]][["groups"]],
          .(value = sum(value)),
          by = .(scenario_id, survey_date, measure)
        ][
          , age_group := if (!is.na(list[[i]][["adj_name"]])) {
            list[[i]][["adj_name"]]
          } else {
            names(list[i])
          }
        ]
      }, list = selAges)
    )
    ## Run garbage collector
    if (use.gc) {
      rm(groups, ages, tmp, selAges)
      gc()
    }
  }

  ## Date aggregation
  if (!is.null(aggregateByDate)) {
    ## Join the aggregated column from the dictionary so can apply the correct
    ## modifications.
    df <- df[omOutputDict(),
      aggregated := i.aggregated,
      on = c(measure = "measure_name")
    ]
    ## We create the resulting data table by creating the individual sub-data
    ## tables and then using rbindlist.
    df <- data.table::rbindlist(l = list(
      if ("year" %in% aggregateByDate) {
        data.table::rbindlist(list(
          df[aggregated == TRUE, .(value = sum(value)),
            by = .(scenario_id,
              date = data.table::year(as.Date(survey_date)),
              measure, age_group
            )
          ],
          df[aggregated == FALSE, .(value = mean(value)),
            by = .(scenario_id,
              date = data.table::year(as.Date(survey_date)),
              measure, age_group
            )
          ]
        ))[
          , c("date", "date_aggregation") := .(
            paste0(date, "-12-31"), rep("year", times = nrow(.SD))
          )
        ]
      },
      if ("month" %in% aggregateByDate) {
        data.table::rbindlist(list(
          df[aggregated == TRUE, .(value = sum(value)),
            by = .(scenario_id,
              year = data.table::year(as.Date(survey_date)),
              month = data.table::month(as.Date(survey_date)),
              measure, age_group
            )
          ],
          df[aggregated == FALSE, .(value = mean(value)),
            by = .(scenario_id,
              year = data.table::year(as.Date(survey_date)),
              month = data.table::month(as.Date(survey_date)),
              measure, age_group
            )
          ]
        ))[
          , c(
            "date", "date_aggregation", "year", "month"
          ) := .(
            paste0(year, "-", sprintf("%02d", month), "-15"),
            rep("month", times = nrow(.SD)), NULL, NULL
          )
        ]
      },
      if (is.null(aggregateByDate) || "survey" %in% aggregateByDate) {
        data.table::rbindlist(list(
          df[aggregated == TRUE, .(value = sum(value)),
            by = .(scenario_id, survey_date, measure, age_group)
          ],
          df[aggregated == FALSE, .(value = mean(value)),
            by = .(scenario_id, survey_date, measure, age_group)
          ]
        ))[
          , c(
            "date", "date_aggregation", "survey_date"
          ) := .(
            as.character(survey_date), rep("survey", times = nrow(df)), NULL
          )
        ]
      }
    ), use.names = TRUE)
    ## Run garbage collector
    if (use.gc) {
      gc()
    }
  }

  ## Transform to wide format
  ## TODO Cohorts need to be added
  df <- data.table::dcast(
    df,
    scenario_id + date_aggregation + date + age_group ~ measure,
    value.var = "value"
  )

  ## Calculate indicators
  if ("incidenceRate" %in% indicators) {
    df[, incidenceRate := (nUncomp + nSevere) / nHost]
  }

  if ("incidenceRatePerThousand" %in% indicators) {
    df[, incidenceRatePerThousand := ((nUncomp + nSevere) / nHost) * 1000]
  }

  if ("prevalenceRate" %in% indicators) {
    df[, prevalenceRate := nPatent / nHost]
  }

  if ("tUncomp" %in% indicators) {
    df[, tUncomp := nTreatments1 + nTreatments2]
  }

  if ("tSevere" %in% indicators) {
    df[, tSevere := nTreatments3]
  }

  if ("nHosp" %in% indicators) {
    df[, nHosp := nHospitalDeaths + nHospitalRecovs + nHospitalSeqs]
  }

  if ("edeath" %in% indicators) {
    df[, edeath := expectedDirectDeaths + expectedIndirectDeaths]
  }

  if ("edeathRatePerHundredThousand" %in% indicators) {
    df[, edeathRatePerHundredThousand := (expectedDirectDeaths + expectedIndirectDeaths) / nHost * 1e5]
  }

  if ("edirdeath" %in% indicators) {
    df[, edirdeath := expectedDirectDeaths]
  }

  if ("edirdeathRatePerHundredThousand" %in% indicators) {
    df[, edirdeathRatePerHundredThousand := expectedDirectDeaths / nHost * 1e5]
  }

  if ("ddeath" %in% indicators) {
    df[, ddeath := nIndDeaths + nDirDeaths]
  }

  if ("ddeathRatePerHundredThousand" %in% indicators) {
    df[, ddeathRatePerHundredThousand := (nIndDeaths + nDirDeaths) / nHost * 1e5]
  }

  ## Drop non-requested columns
  df <- df[, (dropCols) := NULL]
  ## Make sure the date is a string (Needed for SQLite)
  df <- df[, date := as.character(date)]

  return(df)
}
