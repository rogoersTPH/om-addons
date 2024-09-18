## TODO Expose the pre, cmd and post options

##' @title Run preparations for SLURM submission for postprocessing
##' @param expName Name of experiment
##' @param dbName Name of sqlite data base file of output results, if null
##' search of sqlite file in rootDir
##' @param bSize Number of parallely processed scenario outputs, if null defaults
##' to batching used for scenarios
##' @param ntasks Number of tasks per CPU
##' @param nCPU Number of cores to reserve
##' @param memCPU Memory per CPU
##' @param time Maximum time
##' @param qos Quality of service
##' @param readResults Extract data and overwrites sqlite file while creating
##' data base from output files, same as slurmPrepareResults, slurmRunResults
##' @param experiment_id Numeric vector of experiment ids to process
##' @param scenario_id Numeric vector of scenario ids to process
##' @param metadataFeatures Character vector of metadata features (patterns are
##' matched) to keep, if null keep all
##' @param indicators Character vector of epidemiological indicators to calculate
##' @param customIndicatorFormula Character vector to parse and evaluate for
##' custom indicators
##' @param keepMeasuresForIndicators Character vector of indicators whose
##' measures we want to keep
##' @param aggregateByAgeGroup Character vector of age groups (e.g. "2-10" for
##' the right-open interval) to be
##' used for aggregation, if NULL, then age groups from surveys will be used
##' @param aggregateByDate List (named "month", "year", "date") list(month="month",year="year",date=as.Date(c("2010-04-06","2012-04-06"))) to
##' be used for aggregation, if NULL, then survey dates will be used, for month, aggregated survey date is set to middle of month
##' @param wideFormat Logical vector, if TRUE than output in wide format
##' @param timeHorizon Character vector of start date and end date of
##' time horizon to be processed, by default start on 2000-01-01

##' @export
slurmPreparePostprocessing <- function(expName, dbName, bSize = 200,
                                       ntasks = 1, memCPU = "32G", nCPU = 1,
                                       time = "06:00:00", qos = "6hours",
                                       readResults = FALSE,
                                       experiment_id = 1,
                                       scenario_id = NULL,
                                       indicators = c("simulatedEIR", "incidence", "prevalence"),
                                       customIndicatorFormula = list("incidencePerFifty" = "(nUncomp+nSevere)/nHost*50"),
                                       metadataFeatures = c("setting", "EIRid"), keepMeasuresForIndicators = NULL,
                                       aggregateByAgeGroup = c("0-5", "2-10"),
                                       aggregateByDate = list(month = "month", year = "year", date = as.Date(c("2010-04-06", "2012-04-06", "2015-01-06", "2030-04-06"))),
                                       wideFormat = TRUE, timeHorizon = c("2000-01-01", "2100-01-01")) {
  ## TODO parallelize with ncores


  ## Calculate number of scenarios and divide them into packages of bSize per
  ## experiment_id
  openMalariaUtilities::loadExperiment(openMalariaUtilities::getCache(x = "experimentDir"))

  # ## Calculate number of scenarios and divide them into packages of bSize
  if (is.null(bSize)) {
    openMalariaUtilities::getCache(x = "slurm_scenarios") -> batching
    nbatches <- batching$scen_nbatches
    batches <- batching$scen_batches
  } else {
    openMalariaOutput <- DBI::dbConnect(RSQLite::SQLite(), file.path(openMalariaUtilities::getCache(x = "rootDir"), paste0(dbName, ".sqlite")))
    scenarios_metadata <- DBI::dbReadTable(openMalariaOutput, "scenarios_metadata")
    nscens <- as.integer(unique(scenarios_metadata$scenario_id))
    batches <- openMalariaUtilities::splitSeq(nscens, bSize)
    nbatches <- length(batches)
  }



  ## Store into cache
  temp <- list(
    expName = expName,
    scen_batches = batches,
    scen_nbatches = nbatches
  )
  openMalariaUtilities::putCache(x = "slurm_postprocessing", value = temp)
  openMalariaUtilities::syncCache(
    path = openMalariaUtilities::getCache(x = "experimentDir")
  )
  ## Create a submission script
  filename <- file.path(
    openMalariaUtilities::getCache(x = "experimentDir"), "slurm_postprocessing.sh"
  )

  dir.create(file.path(openMalariaUtilities::getCache(x = "logsDir"), "postprocessing"))
  dir.create(file.path(openMalariaUtilities::getCache(x = "experimentDir"), "postprocessing"))

  .writeSlurm(
    jobName = paste0(expName, "_postprocessing"),
    ntasks = ntasks,
    memCPU = memCPU,
    nCPU = nCPU,
    array = nbatches,
    time = time,
    qos = qos,
    output = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "postprocessing"),
      paste0("slurm_", expName, "_postprocessing")
    ),
    error = file.path(
      file.path(openMalariaUtilities::getCache(x = "logsDir"), "postprocessing"),
      paste0("slurm_", expName, "_postprocessing")
    ),
    pre = list(
      "module purge",
      "module load R/4.1.2-foss-2018b-Python-3.6.6"
    ),
    cmd = list(paste("Rscript", file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_postprocessing.R"
    ), "$ID", "$SLURM_CPUS_PER_TASK")),
    file = filename
  )

  ## Create R script
  cat(
    "#!/usr/bin/env Rscript

## Get arguments
args <- commandArgs(trailingOnly = TRUE)

## Set correct working directory\n",
    "setwd(dir = \"", paste0(openMalariaUtilities::getCache(x = "rootDir")), "\")

## Load library
library(openMalariaUtilities)
library(OMAddons)
library(magrittr)
library(data.table)

## Load cached data
loadExperiment(\"", paste0(openMalariaUtilities::getCache(x = "experimentDir")), "\")

## Get scenario numbers to postprocess
slurm <- openMalariaUtilities::getCache(x = \"slurm_postprocessing\")
ID <- args[1]
ncores <- as.numeric(args[2])
rowStart <- slurm$scen_batches[[ID]][1]
rowEnd <- slurm$scen_batches[[ID]][length(slurm$scen_batches[[ID]])]

OMAddons::postprocess(experimentDir=openMalariaUtilities::getCache(x = \"experimentDir\"),
            dbName=", noquote(deparse(dbName)),
    ",\n experiment_id=", noquote(deparse(experiment_id)),
    ",\n scenario_id=c(rowStart:rowEnd)",
    ",\n batch_id=ID",
    ",\n readResults=", noquote(deparse(readResults)),
    ",\n indicators=", noquote(deparse(indicators)),
    ",\n customIndicatorFormula=", noquote(deparse(customIndicatorFormula)),
    ",\n metadataFeatures=", noquote(deparse(metadataFeatures)),
    ",\n keepMeasuresForIndicators=", noquote(deparse(keepMeasuresForIndicators)),
    ",\n aggregateByAgeGroup=", noquote(deparse(aggregateByAgeGroup)),
    ",\n aggregateByDate=", noquote(deparse(aggregateByDate)),
    ",\n wideFormat=", noquote(deparse(wideFormat)),
    ",\n outputFilename=file.path(openMalariaUtilities::getCache(x = \"experimentDir\"),'postprocessing',paste0(", noquote(deparse(dbName)), ",\"-postprocessed-\",ID,\".rds\"))",
    ")",

    # "\n## Load postprocessed batches and merge
    # current_files <- list.files(pattern = paste0(",noquote(deparse(dbName)),",\"-postprocessed-[0-9]*.rds$\"),full.names=TRUE)
    # print(current_files)
    #
    # if (length(current_files)>0){
    # mergedat <- do.call('rbind', lapply(current_files, readRDS))
    #
    # if (file.exists(paste0(",noquote(deparse(dbName)),",\"-postprocessed.rds\"))){
    #   dat<-readRDS(paste0(",noquote(deparse(dbName)),",\"-postprocessed.rds\"))
    #   dat<-rbind(dat,mergedat)
    #   saveRDS(dat,paste0(",noquote(deparse(dbName)),",\"-postprocessed.rds\"))
    # }else{
    #   saveRDS(mergedat,paste0(",noquote(deparse(dbName)),",\"-postprocessed.rds\"))
    # }
    # file.remove(current_files)}",
    file = file.path(
      openMalariaUtilities::getCache(x = "experimentDir"), "slurm_run_postprocessing.R"
    ),
    sep = ""
  )
}



##' @title Submit postprocessing job to SLURM
##' @param wait If TRUE, use the '--wait' flag
##' @export
slurmRunPostprocessing <- function(wait = FALSE) {
  system(
    command = paste0(
      "sbatch ", if (wait == TRUE) {
        "--wait "
      }, file.path(
        openMalariaUtilities::getCache("experimentDir"),
        "slurm_postprocessing.sh"
      )
    )
  )
}

##' @title Generate top section of SLURM script
##' @param jobName Name of the job
##' @param ntasks Number of tasks
##' @param nCPU Number of cores to reserve
##' @param memCPU RAM per CPU
##' @param output Output file
##' @param error Error log file
##' @param array Array range
##' @param time Maximum time
##' @param qos Quality of service
##' @keywords internal
.slurmOptions <- function(jobName, ntasks = NULL, nCPU = NULL, memCPU = NULL,
                          output = NULL, error = NULL, array = NULL,
                          time = NULL, qos = NULL) {
  ## Header and job name
  paste0(
    "#!/bin/bash", "\n",
    "#SBATCH --job-name=", jobName, "\n",
    ## Number of tasks per CPU
    if (!is.null(ntasks)) {
      paste0("#SBATCH --ntasks=", ntasks, "\n")
    },
    ## Number of CPUs per tasks
    if (!is.null(nCPU)) {
      paste0("#SBATCH --cpus-per-task=", nCPU, "\n")
    },

    ## RAM per CPU
    if (!is.null(memCPU)) {
      paste0("#SBATCH --mem-per-cpu=", memCPU, "\n")
    },
    ## Output log file destination
    if (!is.null(output)) {
      paste0("#SBATCH --output=", output, "_%A_%a.log", "\n")
    },
    ## Error log file destination
    if (!is.null(error)) {
      paste0("#SBATCH --error=", error, "_%A_%a_error.log", "\n")
    },
    ## Job array
    if (!is.null(array)) {
      paste0("#SBATCH --array=1-", array, "\n")
    },
    ## Time limit
    if (!is.null(time)) {
      paste0("#SBATCH --time=", time, "\n")
    },
    ## Quality of service
    if (!is.null(qos)) {
      paste0("#SBATCH --qos=", qos, "\n")
    },
    sep = ""
  )
}

##' @title Write SLURM script to file
##' @param jobName Name of the job
##' @param ntasks Number of tasks
##' @param memCPU RAM per CPU
##' @param nCPU Number of cores to reserve
##' @param output Output file
##' @param error Error log file
##' @param array Array range
##' @param time Maximum time
##' @param qos Quality of service
##' @param pre List of commands before main command
##' @param cmd Main command
##' @param post List of commands after main command
##' @param file Filename
##' @keywords internal
.writeSlurm <- function(jobName, ntasks = NULL, nCPU = NULL, memCPU = NULL,
                        output = NULL, error = NULL, array = NULL, time = NULL,
                        qos = NULL, pre = NULL, cmd = NULL, post = NULL,
                        file = NULL) {
  cat(
    .slurmOptions(
      jobName = jobName, ntasks = ntasks, nCPU = nCPU, memCPU = memCPU,
      output = output, error = error, array = array, time = time, qos = qos
    ),
    if (!is.null(array)) {
      paste0("ID=$(expr ${SLURM_ARRAY_TASK_ID} - 0)", "\n")
    },
    "\n",
    if (!is.null(pre)) {
      paste0(unlist(pre), "\n")
    },
    "\n",
    if (!is.null(cmd)) {
      paste0(unlist(cmd), "\n")
    },
    "\n",
    if (!is.null(post)) {
      paste0(unlist(post), "\n")
    },
    sep = "",
    file = if (!is.null(file)) {
      file
    } else {
      ""
    }
  )
}
