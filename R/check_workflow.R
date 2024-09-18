# This file contains functions for checking that various parts of the workflow
# were completed successfully (e.g., scenarios creation, simulations, etc.)

##' @title checks if the defined scenarios for an experiment were created
##' @param experiment_folder folder with the workflow outputs of an experiment
##' @param stop_if_missing if TRUE the function returns an error message and
##' stops further downstream operations if any scenarios are missing;
##' default value is FALSE
##' @export
##' @return if there are missing scenarios, it creates a file,
##' missing_scenarios.txt, containing the file names of the missing scenarios
check_scenarios_created <- function(experiment_folder, stop_if_missing = FALSE) {
  # Identify the scenarios folder and data frame
  scenario_folder <- file.path(experiment_folder, "/scenarios/")
  scenario_rds_file <- file.path(experiment_folder, "/cache/scenarios.rds")

  # Check if the scenario folder and data frame are not empty
  if (!file.exists(scenario_folder)) {
    stop("The folder with scenarios does not exist!")
  }
  if (!file.exists(scenario_rds_file)) {
    stop("Scenario table (scenarios.rds) does not exist!")
  }

  # Extract the generated scenarios
  scenario_list_created <- list.files(path = scenario_folder, pattern = "xml")
  created_scenarios <- length(scenario_list_created)
  # Extract the full set of defined scenarios that should have been generated
  scenario_list_required <- readRDS(scenario_rds_file)
  required_scenarios <- nrow(scenario_list_required)
  if (!(required_scenarios > 0)) {
    stop("No scenarios stored for the experiment, scenarios.rds is empty!")
  }
  # Create a file to store the file names of the missing scenarios
  missing_file <- file.path(experiment_folder, "/missing_scenarios.txt")

  if (created_scenarios == required_scenarios) {
    print("All scenarios were successfully created.")

    # If the missing file was previously created, remove it
    if (file.exists(missing_file)) {
      file.remove(missing_file)
    }
  } else {
    # Retrieve the missing scenarios and save their file names to a file
    ind_missing <- which(
      !(scenario_list_required$file %in% scenario_list_created)
    )
    list_missing <- scenario_list_required$file[ind_missing]
    write(list_missing, missing_file)
    err_msg <- paste(
      "Not all scenarios were generated.",
      "Number of missing scenarios: ",
      abs(created_scenarios - required_scenarios),
      ". Check the missing_scenarios.txt to see which scenarios",
      "are missing", "and if using the cluster, check the",
      "logs/scenarios/ folder for error",
      "messages encountered during the scenario creation."
    )

    if (stop_if_missing) {
      stop(err_msg)
    } else {
      print("WARNING:")
      message(err_msg)
    }
  }
}

##' @title checks if the OpenMalaria simulations for an experiment were
##' successfully completed
##' @param experiment_folder folder with the workflow outputs of an experiment
##' @param stop_if_missing if TRUE the function returns an error message and
##' stops further downstream operations if any simulations failed;
##' default value is FALSE
##' @importFrom stringr str_replace
##' @export
##' @return if there are missing simulations, it creates a file,
##' missing_om_simulations.txt, containing the file names of
##' the failed simulations
check_simulations_created <- function(experiment_folder,
                                      stop_if_missing = FALSE) {
  # Create a file to store the job IDs of the missing simulations
  missing_om_file <- file.path(experiment_folder, "/missing_om_simulations.txt")

  # Identify the scenarios folder and data frame
  simulation_folder <- file.path(experiment_folder, "/outputs/")
  scenario_rds_file <- file.path(experiment_folder, "/cache/scenarios.rds")

  # Check if the scenario folder and data frame are not empty
  if (!file.exists(simulation_folder)) {
    stop("The folder with simulation outputs does not exist!")
  }
  if (!file.exists(scenario_rds_file)) {
    stop("Scenario table (scenarios.rds) does not exist!")
  }

  # Extract the full set of defined scenarios that should have been generated
  scenario_list_required <- readRDS(scenario_rds_file)
  required_scenarios <- nrow(scenario_list_required)
  if (!(required_scenarios > 0)) {
    stop("No scenarios stored for the experiment, scenarios.rds is empty!")
  }

  simulation_list_created <- list.files(
    path = simulation_folder,
    pattern = "_out.txt"
  )
  created_simulations <- length(simulation_list_created)

  if (created_simulations == required_scenarios) {
    print("All OpenMalaria simulations were successfully completed.")

    # If the missing file was previously created, remove it
    if (file.exists(missing_om_file)) {
      file.remove(missing_om_file)
    }
  } else {
    required_simulation_files <- stringr::str_replace(scenario_list_required$file, ".xml", "_out.txt")
    ind_missing <- which(!(required_simulation_files %in% simulation_list_created))
    list_missing <- required_simulation_files[ind_missing]
    write(list_missing, missing_om_file)
    err_msg <- paste(
      "Not all openMalaria simulations were generated.",
      "Number of missing simulations: ",
      length(list_missing),
      ". Check the missing_om_simulations.txt to see which",
      "simulations are missing and if using the cluster,",
      "the logs/simulation/ folder",
      "for the corresponding slurm job outputs."
    )

    if (stop_if_missing) {
      stop(err_msg)
    } else {
      print("WARNING:")
      message(err_msg)
    }
  }
}
