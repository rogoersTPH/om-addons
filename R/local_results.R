.datatable.aware <- TRUE

## TODO Expose the pre, cmd and post options

##' @title Run results collection
##' @param expDir Directory of experiment
##' @param dbName Name of the database to create
##' @param dbDir Directory of the database file. Defaults to the root directory.
##' @param resultsName Name of the database table to add the results to.
##' @param resultsCols Vector of column names.
##' @param indexOn Define which index to create. Needs to be a lis of the form
##'   list(c(TABLE, COLUMN), c(TABLE, COLUMN), ...).
##' @param ncoresDT Number of data.table threads to use on each parallel
##'   cluster.
##' @param strategy Defines how to process the files. "batch" means that all
##'   files are read into a single data frame first, then the aggregation
##'   funciton is applied to that data frame and the result is added to the
##'   database. "serial" means that each individual file is processed with the
##'   aggregation function and added to the database.
##' @param appendResults If TRUE, do not add metadata to the database and only
##'   write results.
##' @param aggrFun A function for aggregating the output of readFun. First
##'   argument needs to be the output data frame of readFun and it needs to
##'   generate a data frame. The data frame should NOT contain an experiment_id
##'   column as this is added automatically. The column names needs to match the
##'   ones defined in resultsCols.
##' @param aggrFunArgs Arguments for aggrFun as a (named) list.
##' @param splitBy Split total number of scenarios into groups. Either a numeric
##'   value or a column name from your scenarios.
##' @param nCPU Number of cores to reserve
##' @param verbose Toggle verbose output
##' @export
runResults <- function(expDir, dbName, dbDir = NULL,
                       resultsName = "results", resultsCols = c(
                         "scenario_id", "date_aggregation", "date",
                         "age_group", "incidenceRate",
                         "prevalenceRate"
                       ),
                       indexOn = list(c("results", "scenario_id")),
                       ncoresDT = 1, strategy = "batch",
                       appendResults = FALSE,
                       aggrFun = CalcEpiOutputs,
                       aggrFunArgs = list(
                         aggregateByAgeGroup = c("0-5", "2-10", "0-100"),
                         aggregateByDate = "year"
                       ),
                       splitBy = 10000,
                       verbose = FALSE,
                       nCPU = 1) {
  ## Get path if not given
  if (is.null(dbDir)) {
    dbDir <- openMalariaUtilities::getCache("rootDir")
  }

  ## Remove database
  unlink(file.path(dbDir, paste0(dbName, ".sqlite")))
  unlink(file.path(dbDir, paste0(dbName, ".sqlite-shm")))
  unlink(file.path(dbDir, paste0(dbName, ".sqlite-wal")))

  ## TODO chohorts and co
  coltbl <- data.table::data.table(
    names = c(
      "scenario_id", "date_aggregation", "date",
      "age_group",
      ## calculated indicators
      "prevalenceRate", "incidenceRate", "incidenceRatePerThousand",
      "tUncomp", "tSevere", "nHosp", "edeath",
      "edeathRatePerHundredThousand", "edirdeath",
      "edirdeathRatePerHundredThousand", "ddeath",
      "ddeathRatePerHundredThousand",
      ## OM measures
      openMalariaUtilities::omOutputDict()[["measure_name"]]
    ),
    types = c(
      "INTEGER", "TEXT", "TEXT", "TEXT",
      ## calculated indicators
      rep("NUMERIC", times = 12),
      ## OM measures
      rep("NUMERIC",
        times = length(
          openMalariaUtilities::omOutputDict()[["measure_name"]]
        )
      )
    )
  )

  ## Get column types by making a join
  resultsCols <- data.table::data.table(names = resultsCols)
  resultsCols <- coltbl[resultsCols, on = "names"]

  ## Add requested columns to aggrFunArgs and store everything in the cache
  aggrFunArgs[["indicators"]] <- resultsCols[["names"]]
  funs <- list(
    cAggrFun = aggrFun, cAggrFunArgs = aggrFunArgs
  )

  ## Create R script
  if (is.character(splitBy)) {
    ## Set correct working directory\n",
    setwd(dir = openMalariaUtilities::getCache(x = "rootDir"))

    ## Read scenarios
    scens <- openMalariaUtilities::readScenarios()

    fileCol <- function(scens, cname) {
      filesAggr <-
        scens[eval(parse(text = paste0(as.symbol(splitBy), " == ", cname))), file]
      return(filesAggr)
    }

    for (s in unique(scens[[splitBy]])) {
      message(paste0("Working on ", s))

      openMalariaUtilities::collectResults(
        expDir = expDir,
        dbName = dbName,
        dbDir = dbDir,
        replace = FALSE,
        fileFun = fileCol, fileFunArgs = list(scens = scens, cname = s),
        aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
        ncores = nCPU, ncoresDT = ncoresDT,
        strategy = strategy,
        resultsName = paste0("results_", s),
        resultsCols = resultsCols,
        indexOn = indexOn,
        verbose = verbose
      )

      gc(verbose = TRUE)
    }
  } else if (is.numeric(splitBy)) {
    ## Set correct working directory\n",
    setwd(dir = openMalariaUtilities::getCache(x = "rootDir"))

    ## Read scenarios
    scens <- openMalariaUtilities::readScenarios()
    batches <- splitSeq(1:nrow(scens), n = splitBy)


    fileCol <- function(scens, ffilter) {
      scens <- data.table::as.data.table(scens)
      ffilter <- substitute(ffilter)
      fToUse <- scens[eval(ffilter), file]
      return(fToUse)
    }

    for (batch in names(batches)) {
      lower_scen <- min(batches[[batch]])
      upper_scen <- max(batches[[batch]])

      message(paste0("Working on scenario ", lower_scen, " to ", upper_scen))

      openMalariaUtilities::collectResults(
        expDir = expDir,
        dbName = dbName,
        dbDir = dbDir,
        replace = FALSE,
        fileFun = fileCol, fileFunArgs = list(scens = scens, ffilter = batches[[batch]]),
        aggrFun = funs$cAggrFun, aggrFunArgs = funs$cAggrFunArgs,
        ncores = nCPU, ncoresDT = ncoresDT,
        strategy = strategy,
        resultsName = resultsName,
        resultsCols = resultsCols,
        indexOn = indexOn,
        verbose = verbose
      )

      gc(verbose = TRUE)
    }
  } else {
    stop("splitBy needs be a number or a column name.")
  }
}
