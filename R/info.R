##' @title Collect some infos about the experiment
##' @export
collectStats <- function(scenj = NULL, simj = NULL, postj = NULL) {
  tryCatch(
    {
      expDir <- openMalariaUtilities::getCache("experimentDir")
    },
    error = function(c) {
      message("Could not detect Experiment. Run loadExperiment first.")
    }
  )

  ## Collect information from OMU
  scens <- openMalariaUtilities::readScenarios()
  nScens <- nrow(scens)
  nPl <- length(openMalariaUtilities::getCache("placeholders"))

  nScenLines <- length(
    readLines(
      file.path(
        openMalariaUtilities::getCache("scenariosDir"),
        scens[1, "file"]
      )
    )
  )

  scen_nbatches <- openMalariaUtilities::getCache("slurm_scenarios")[["scen_nbatches"]]
  scen_lbatches <- length(openMalariaUtilities::getCache("slurm_scenarios")[["scen_batches"]][[1]])

  ## Population size
  pop_size <- gsub(pattern = ".*popSize=\"([0-9]+?)\".*", "\\1", paste(readLines(
    file.path(
      openMalariaUtilities::getCache("scenariosDir"),
      scens[1, "file"]
    )
  ), collapse = " "))


  ## Collect OM output information
  outputLines <- length(
    readLines(
      file.path(
        openMalariaUtilities::getCache("outputsDir"),
        gsub(".xml", "_out.txt", scens[1, "file"])
      )
    )
  )
  sim_nbatches <- openMalariaUtilities::getCache("slurm_simulation")[["scen_nbatches"]]
  sim_lbatches <- length(openMalariaUtilities::getCache("slurm_simulation")[["scen_batches"]][[1]])

  ## Collect job stats
  if (!is.null(scenj)) {
    scenjob <- read.table(text = system(paste0("sacct -j ", scenj, " --format AllocCPUS,MaxRSS --noheader --parsable2 --noconvert"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|", )
    scentimes <- read.table(text = system(paste0("sacct -j ", scenj, " --format=start,end --noheader --parsable2 -X"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|")
    scentimes <- as.data.frame(lapply(scentimes, as.POSIXct, format = "%Y-%m-%dT%H:%M:%S"))
  }
  if (!is.null(simj)) {
    simjob <- read.table(text = system(paste0("sacct -j ", simj, " --format AllocCPUS,MaxRSS --noheader --parsable2 --noconvert"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|")
    simtimes <- read.table(text = system(paste0("sacct -j ", simj, " --format=start,end --noheader --parsable2 -X"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|")
    simtimes <- as.data.frame(lapply(simtimes, as.POSIXct, format = "%Y-%m-%dT%H:%M:%S"))
  }
  if (!is.null(postj)) {
    postjob <- read.table(text = system(paste0("sacct -j ", postj, " --format AllocCPUS,MaxRSS --noheader --parsable2 --noconvert"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|")
    posttimes <- read.table(text = system(paste0("sacct -j ", postj, " --format=start,end --noheader --parsable2 -X"),
      intern = TRUE, ignore.stderr = TRUE
    ), header = FALSE, sep = "|")
    posttimes <- as.data.frame(lapply(posttimes, as.POSIXct, format = "%Y-%m-%dT%H:%M:%S"))
  }

  if (!is.null(scenj)) {
    cat("\n# Scenarios:\n")
    print(data.frame(
      n = nScens,
      n_lines = nScenLines,
      n_placeholders = nPl,
      n_batches = scen_nbatches,
      b_size = scen_lbatches,
      n_cpu = as.numeric(scenjob[1, 1]),
      mem_used = round(max(scenjob[, 2], na.rm = TRUE) / (1024 * 1024 * 1024), 2),
      time_avg = round(mean(apply(scentimes, 1, function(x) difftime(x[2], x[1], units = "min"))), 2),
      time_tot = round(difftime(max(scentimes[, 2]), min(scentimes[, 1]), units = "min"), 2)
    ))
  }

  if (!is.null(simj)) {
    cat("\n# Simulations:\n")
    print(data.frame(
      n = nScens,
      n_batches = sim_nbatches,
      b_size = sim_lbatches,
      pop_size = as.numeric(pop_size),
      n_cpu = as.numeric(simjob[1, 1]),
      mem_used = round(max(simjob[, 2], na.rm = TRUE) / (1024 * 1024 * 1024), 2),
      time_avg = round(mean(apply(simtimes, 1, function(x) difftime(x[2], x[1], units = "min"))), 2),
      time_tot = round(difftime(max(simtimes[, 2]), min(simtimes[, 1]), units = "min"), 2)
    ))
  }

  if (!is.null(postj)) {
    cat("\n# Post:\n")
    print(data.frame(
      n = nScens,
      n_output_lines = outputLines,
      n_cpu = as.numeric(postjob[1, 1]),
      mem_used = round(max(postjob[, 2], na.rm = TRUE) / (1024 * 1024 * 1024), 2),
      time_avg = round(mean(apply(posttimes, 1, function(x) difftime(x[2], x[1], units = "min"))), 2),
      time_tot = round(difftime(max(posttimes[, 2]), min(posttimes[, 1]), units = "min"), 2)
    ))
  }
}
