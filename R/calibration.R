#' @title Retrieve postprocessed results and prepare for calibration
#' @param experimenName experiment name relative to working directory, if null then obtain from experimentDir
#' @param workingDir working directory, in null then current working directory
#' @param experimentDir full path of experiment directory
#' @param EIR_name the name you gave to scaledAnnualEIR in your scenarios table
#' @return Dataframe
#' @export
get_all_OMsimul <- function(experimentName = NULL, workingDir = NULL, experimentDir = NULL, EIR_name = "scaledAnnualEIR") {
  if (is.null(experimentDir)) {
    if (is.null(workingDir)) {
      workingDir <- getwd()
    }
    setwd(workingDir)
    loadCache(experimentName)
    experimentDir <- getCache("experimentDir")
    experimentName <- getCache("experimentName")
  } else {
    loadCache(experimentDir)
    experimentName <- getCache("experimentName")
  }
  postprocessed_path <- file.path(
    experimentDir, "postprocessing",
    paste0(experimentName, "-postprocessed.rds")
  )
  message(paste0("Loading postprocessed data from: ", postprocessed_path))
  df <- readRDS(postprocessed_path)
  modelOutput <- df %>%
    filter(third_dimension_name == "age_group" & date > "2000-01-01" & dateAggregation == "year") %>%
    rename(
      age = third_dimension_level,
      PR = prevalenceRate
    ) %>%
    mutate(
      EIR_num = as.numeric(get(EIR_name)),
      EIR = get(EIR_name),
      futITNcov = 0,
      year = as.numeric(format(date, "%Y"))
    ) %>%
    select(-c(third_dimension_name, dateAggregation))
  return(modelOutput)
}



#' @title Calculate the likelihood of the simulation with the data, for all EIR
#' @param simul_df A dataframe containing all the OpenMalaria simulations
#'   (output of get_all_OMsimul)
#' @param data_to_fit The dataset to which the simulations are fitted
#' @return A list of 3 dataframes:
#'   1) Best_EIR_ML_gaussian" has the simulations with the best EIR according
#'      to the gaussian likelihood best_EIR_ML_laplace"
#'   2) Has the simulations with the best EIR according to the laplace
#'      likelihood
#'   3) All_llk includes all the simulations and their associated likelihood
#'      values
#' @export
get_best_EIR <- function(simul_df, data_to_fit = fitdat) {
  ## calculate likelihood or least square distance
  simple_dat2 <- simul_df %>%
    dplyr::filter(
      age %in% unique(data_to_fit$age),
      futITNcov == 0
      ## TODO actually, I think there is a way to make this generic
      ##
      ## REVIEW: What is the reason behind futITNcov == 0? the
      ## reason is that we need to keep only the past values and we
      ## want no future intervention (we are calibrating only to the
      ## past) I think the best way to fix this would be to say that
      ## all variables starting with fut or futcov should be put to
      ## zero you can take a look at what Tatiana and Jeanne did,
      ## because they had to adapt this part to their own code
      ## (maybe they did not have futITNcov ro they had other
      ## variables they selected upon) I have moved it here so that
      ## there is no need ot repeat it in the next functions
    ) %>%
    dplyr::left_join(data_to_fit) %>%
    dplyr::mutate(
      ## TODO Allow the user to choose between PR or Incidence (or maybe more?)
      ##      Look at "fittingVar" in munirflow, which does something similar
      ## squared_err = (PR - PR_obs)^2, # used as sanity check for llk, could
      ##                                # be removed to simplify
      ## abs_err = base::abs(PR - PR_obs), # used as sanity check for llk_AE,
      ##                                   # could be removed to simplify
      ## REVIEW The previous two lines could be removed, however they can be
      ## used as a sanity check, as they should produce the same maximum value
      llk_gaussian = stats::dnorm(PR_obs, mean = PR, sd = sd, log = TRUE),
      llk_laplace = extraDistr::dlaplace(PR_obs,
        mu = PR,
        sigma = sd * base::sqrt(2),
        log = TRUE
      )
    ) %>%
    ## FIXME Settings and setting is redundant! and check the grouping variables
    dplyr::group_by(
      sub, setting,
      EIR, EIR_num, seed, futITNcov
    ) %>%
    dplyr::summarise(
      # SE = base::sum(squared_err), #could be removed to simplify
      # AE = base::sum(abs_err),   #could be removed to simplify
      llk_gaussian = base::sum(llk_gaussian),
      llk_laplace = base::sum(llk_laplace)
    )

  ## TODO : In the future, add Poisson and Negative binomial likelihood too
  ##        (useful for incidence)
  ## TODO : In the future, instead of calculating both laplace and gaussian, the
  ##        user can choose which one is calculated

  # ## Extract best EIR according to least squares, could be removed to simplify
  # fitted_EIR_LS <- simple_dat2 %>%
  #   ## TODO make futITNcov user dependent
  #   dplyr::group_by(sub, setting, seed, futITNcov) %>%
  #   dplyr::slice(base::which.min(SE)) %>%
  #   dplyr::left_join(simul_df) %>%
  #   dplyr::left_join(data_to_fit)
  #
  # ## Extract best EIR according to absolute error, could be removed to
  # ## simplify
  # fitted_EIR_AE <- simple_dat2 %>%
  #   ## TODO make futITNcov user dependent
  #   dplyr::group_by(sub, setting, seed, futITNcov) %>%
  #   dplyr::slice(base::which.min(AE)) %>%
  #   dplyr::left_join(simul_df) %>%
  #   dplyr::left_join(data_to_fit)

  ## Extract best EIR according to likelihood (should be the same as
  ## fitted_EIR_LS)
  fitted_EIR_llk_gaussian <- simple_dat2 %>%
    ## TODO  make futITNcov user dependent
    dplyr::group_by(sub, setting, seed, futITNcov) %>%
    dplyr::slice(which.max(llk_gaussian)) %>%
    dplyr::left_join(simul_df) %>%
    dplyr::left_join(data_to_fit)

  ## Extract best EIR according to laplace likelihood (should be the same as
  ## fitted_EIR_AE)
  fitted_EIR_llk_laplace <- simple_dat2 %>%
    ## TODO  make futITNcov user dependent
    dplyr::group_by(sub, setting, seed, futITNcov) %>%
    dplyr::slice(which.max(llk_laplace)) %>%
    dplyr::left_join(simul_df) %>%
    dplyr::left_join(data_to_fit)

  return(list(
    # "best_EIR_LS" = fitted_EIR_LS,
    # "best_EIR_AE" = fitted_EIR_AE,
    "best_EIR_ML_gaussian" = fitted_EIR_llk_gaussian,
    "best_EIR_ML_laplace" = fitted_EIR_llk_laplace,
    "all_llk" = simple_dat2 # full dataste
  ))
}



## Get the quadratic fit, following the methodology
## by Ionides et al. 2017 (Journal of the Royal Society Interface)
##
## The "..." arguments allows the user to define filter criteria which get
## passed to dplyr's filter function
##
## TODO Mention in the documentation that the user probably has to make
##      adjustments to the parameters

#' @title Get the quadratic fit, following the methodology by Ionides et al.
#'   2017
#' @description This function is based on the methodology by Ionides et al. 2017
#'   (https://doi.org/10.1098/rsif.2017.0126) to calculate confidence intervals
#'   based on the approximate profile likelihood, accounting for Monte Carlo
#'   error. It includes a first smoothing step of the likelihood function, using
#'   the loess function. The calculation of the confidence intervals relies on a
#'   local quadratic approximation, where local refers to a selection window
#'   that can be tuned by the user with the parameters select_subset and lambda.
#' @param df Best EIR file, as output of get_best_EIR (could be the
#'   best_EIR_ML_gaussian or the best_EIR_ML_gaussian output)
#' @param df_all The all_llk output of the get_best_EIR function
#' @param confidence Level of statistical confidence for the interval, default
#'   is 95%
#' @param lambda Proportion of the sample to be kept during the smoothing phase
#'   (default is 1)
#' @param select_subset Criterion to select only a subset of the EIR values
#'   which are near the maximum, and to select a larger sample for large EIRs
#'   and a smaller sample for small EIRs. To be adjusted by the user to get an
#'   acceptable quadratic approximation
#' @param llk_type Choose the likelihood to be used (default "llk_gaussian" is
#'   gaussian)
#' @param min_EIRnb_to_keep Minimum number of EIR values to keep, regardless of
#'   the chosen window (default is 10 and should not need to be adjusted in
#'   practice)
#' @param data_to_fit The dataset to which the simulations are fitted
#' @return a dataframe with the fitted quadratic approximation. This output is
#'   needed as input for the function get_profile_llk_ci_approx and is needed to
#'   plot diagnostics on the quality of the quadratic approximation.
#' @export
get_quadratic_fit_cf <- function(df, df_all, confidence = 0.95, lambda = 1,
                                 select_subset = 0.5, llk_type = "llk_gaussian",
                                 data_to_fit = fitdat, min_EIRnb_to_keep = 10) {
  ## Formating the inputs, and applying the first window parameter (select
  ## subset)
  profile_llk <- df %>%
    ## We start with the file containing the best EIR

    ## We select only the relevant variables for calibration (one single year
    ## because the likelihood is already aggregated per year, and only the age
    ## to be fitted)
    dplyr::filter(
      age %in% unique(data_to_fit$age),
      ## TODO In the future, this could also be a month
      year == min(data_to_fit$year),
    ) %>%
    dplyr::select("sub", "EIR_num", llk_type) %>%
    base::unique() %>%
    ## Because it's the best EIR, we rename it and its likelihood to indicate
    ## it's the best
    dplyr::rename("EIRmle" = EIR_num, "maxLLK" = llk_type) %>%
    ## We merge with the full dataset containing all EIRs (not only the best)
    dplyr::left_join(df_all) %>%
    dplyr::filter(
      base::abs(EIR_num - EIRmle) /
        EIRmle < select_subset | base::abs(EIR_num - EIRmle) <= min_EIRnb_to_keep
    )
  ## Criterion to select the local window for quadratic fitting (because
  ## quadratic approximation is only valid locally)

  ## Calculate the smoothed likelihood with loess (includes second window
  ## parameter, lambda)
  smooth_fit <- profile_llk %>%
    ## Grouping by districts
    dplyr::group_by(sub) %>%
    ## Give a generic name to the likelihood
    dplyr::rename("myllk" = llk_type) %>%
    ## Perform the smoothing with loess, including the window parameter lambda
    dplyr::do(
      smoothed_llk = broom::tidy(
        stats::predict(
          stats::loess(
            myllk ~ EIR_num,
            data = ., span = lambda
          )
        )
      )
    ) %>%
    tidyr::unnest(smoothed_llk) %>%
    ## Reformat the results
    base::cbind(EIR_num = profile_llk$EIR_num) %>%
    ## Add a column with the EIR values
    dplyr::rename("smoothed_llk" = x) %>%
    ## Aename the outcome to smoothed_llk
    base::unique() %>%
    dplyr::group_by(sub) %>%
    dplyr::filter(!is.na(smoothed_llk)) %>%
    dplyr::mutate(
      ## Calculate some summary quantities, used for the weighting in the next
      ## step. Basically, we extract/recalculate the weights from the loess
      ## fitting, to be able to incorporate them in the linear fitting
      ## Best EIR using smoothed likelihood
      smooth_arg_max = EIR_num[base::which.max(smoothed_llk)],
      ## Best value of the smoothed likelihood
      smooth_max = smoothed_llk[base::which.max(smoothed_llk)],
      ## Distance between the EIR and the best EIR
      dist = base::abs(EIR_num - smooth_arg_max),
      ## Rank of the distance to the best EIR
      rankdist = base::rank(dist),
      ## Maximum rank of the distance to the best EIR
      maxrankdist = base::max(rankdist),
      ## Indicate if the EIR value is included or not based on the lambda
      ## parameter
      included = rankdist / base::max(rankdist) <= lambda
    )

  ## Prepare data for fitting the quadratic linear model
  quadratic_fitprep <- profile_llk %>%
    dplyr::left_join(smooth_fit) %>%
    dplyr::filter(included) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(sub) %>%
    dplyr::mutate(weight = (1 - (dist / base::max(dist))^3)^3) %>%
    ## Calculate the weights based on the loess fitting
    dplyr::rename("myllk" = llk_type)

  ## calculate quadratic linear model, extract the regression parameters
  quadratic_fit_beta <- quadratic_fitprep %>%
    dplyr::do(
      lm.DD = broom::tidy(
        ## Quadratic linear fit
        stats::lm(myllk ~ EIR_num + I(EIR_num^2),
          data = ., weight = weight
        )
      )
    ) %>%
    tidyr::unnest(lm.DD) %>%
    ## Rename the outcomes
    dplyr::mutate(term2 = base::ifelse(term == "(Intercept)", "c",
      base::ifelse(term == "EIR_num", "b",
        base::ifelse(term == "I(EIR_num^2)", "a", NA)
      )
    )) %>%
    ## Reformat the data
    tidyr::pivot_wider(
      id_cols = c(sub),
      names_from = term2,
      values_from = c(estimate, std.error)
    )

  ## Calculate quadratic linear model, extract the covariance
  quadratic_fit_cov <- quadratic_fitprep %>%
    ## Quadratic linear fit and extract the covariance
    dplyr::do(cov_ab = (stats::vcov(lm(myllk ~ EIR_num + I(EIR_num^2),
      data = ., weight = weight
    ))[2, 3])) %>%
    tidyr::unnest(cov_ab)

  ## Combine the results and calculate the delta threshold (cf. Ionides et al.
  ## 2017)
  Quadratic_fit <- quadratic_fit_beta %>%
    dplyr::left_join(profile_llk %>%
      dplyr::left_join(smooth_fit) %>%
      dplyr::filter(included)) %>%
    dplyr::mutate(
      ## Calculate the linear approximation ax^2 +b x + c
      llk_tilde = estimate_c +
        EIR_num * estimate_b +
        EIR_num * EIR_num * estimate_a,
      ## Calculate the optimum of the linear approximation i.e. -b/2a
      EIRmle_tilde = base::ifelse(estimate_a < 0,
        -estimate_b / 2 / estimate_a,
        base::min(EIR_num)
      ),
      ## Calculate the arg optimum of the linear approximation, -b^2/4a +c
      mle_tilde = -(estimate_b)^2 / 4 / estimate_a + estimate_c
    ) %>%
    dplyr::left_join(quadratic_fit_cov) %>%
    ## Calculate se_mc_squared and delta as in Ionides et al. 2017
    dplyr::mutate(
      se_mc_squared = (std.error_b^2 -
        (2 * cov_ab * estimate_b / estimate_a) +
        (estimate_b^2 * std.error_a^2 / estimate_a^2)) /
        4 /
        (estimate_a^2),
      delta = stats::qchisq(confidence, df = 1) *
        (-estimate_a * se_mc_squared + 0.5)
    )

  return(Quadratic_fit %>%
    dplyr::select(
      -smooth_arg_max, -dist, -rankdist,
      -maxrankdist, -included
    ))
}





#' @title Selection of the upper and lower bound on the EIR and merge with the
#'   simulation file
#' @description Post-processes the output of get_quadratic_fit_cf to retrieve the
#'   best EIR and its confidence interval. Additional parameters are required to
#'   specifiy the rules to be applied at the edges of the EIR grid
#' @param my_quadratic_approx Output from the function get_quadratic_fit_cf
#' @param min_possible_EIR Lowest EIR value in the simulated grid (used when the
#'   lower bound of the confidence interval is either negative or the best EIR
#'   is below lower_threshold)
#' @param max_possible_EIR Lowest EIR value in the simulated grid (used when the
#'   best EIR is above upper_threshold)
#' @param lower_threshold Threshold for the best EIR value under which the
#'   min_possible_EIR is used as lower bound of the confidence interval
#' @param upper_threshold Threshold for the best EIR value above which the
#'   max_possible_EIR is used as upper bound of the confidence interval
#' @param upper_threshold2 Threshold for the best EIR value above which the
#'   max_possible_EIR is used as upper bound of the confidence interval, in case
#'   the upper bound is NA
#' @return A dataframe with one line per subsetting, indicating the best EIR
#'   (EIRmle_tilde, alonside its rounded value EIRmle_tilde_round), the upper
#'   bound (EIR_uci) and the lower bound (EIR_lci) of the confidence interval.
#'   It also contains the associated likelihood values.
#' @param inside_window if TRUE, the upper and lower bounds are found inside the window (which leads to less NA values). Default is FALSE
#' @export
get_profile_llk_ci_approx <- function(my_quadratic_approx,
                                      min_possible_EIR = 1,
                                      max_possible_EIR = 200,
                                      lower_threshold = 3,
                                      upper_threshold = 190,
                                      upper_threshold2 = 160,
                                      inside_window = FALSE) {
  ## Input formatting, select all EIRs below the threshold

  if(inside_window){
      profile_llk <- my_quadratic_approx %>%
        dplyr::group_by(
          sub, setting,
          EIR_num, EIRmle_tilde, smoothed_llk, smooth_max, delta
        ) %>%
        dplyr::summarise(EIRmle_mean = base::mean(EIRmle)) %>%
        base::unique() %>%
        dplyr::filter(smoothed_llk >= smooth_max - delta)

      ## Extract the upper bound (UCI=upper confidence interval)
      uci <- profile_llk %>%
        dplyr::filter(EIR_num >= EIRmle_tilde) %>%
        dplyr::group_by(sub, setting) %>%
        dplyr::slice(base::which.min(smoothed_llk)) %>%
        dplyr::select(sub, setting, smoothed_llk, EIR_num)

      ## Extract the lower bound (UCI=lower confidence interval)
      lci <- profile_llk %>%
        dplyr::filter(EIR_num <= EIRmle_tilde) %>%
        dplyr::group_by(sub, setting) %>%
        dplyr::slice(base::which.min(smoothed_llk)) %>%
        dplyr::select(sub, setting, smoothed_llk, EIR_num)
    } else {
      profile_llk <- my_quadratic_approx %>%
        dplyr::group_by(
          sub, setting,
          EIR_num, EIRmle_tilde, smoothed_llk, smooth_max, delta
        ) %>%
        dplyr::summarise(EIRmle_mean = base::mean(EIRmle)) %>%
        base::unique() %>%
        dplyr::filter(smoothed_llk < smooth_max - delta)

      ## Extract the upper bound (UCI=upper confidence interval)
      uci <- profile_llk %>%
        dplyr::filter(EIR_num >= EIRmle_tilde) %>%
        dplyr::group_by(sub, setting) %>%
        dplyr::slice(base::which.max(smoothed_llk)) %>%
        dplyr::select(sub, setting, smoothed_llk, EIR_num)

      ## Extract the lower bound (UCI=lower confidence interval)
      lci <- profile_llk %>%
        dplyr::filter(EIR_num <= EIRmle_tilde) %>%
        dplyr::group_by(sub, setting) %>%
        dplyr::slice(base::which.max(smoothed_llk)) %>%
        dplyr::select(sub, setting, smoothed_llk, EIR_num)
    }






  # Combine all to get the confidence interval, and add ifelse conditions on the
  # edge of the grid
  ci <- my_quadratic_approx %>%
    dplyr::filter(futITNcov == 0) %>%
    dplyr::group_by(sub, setting, EIRmle_tilde, smooth_max) %>%
    dplyr::summarise(EIRmle_mean = base::mean(EIRmle)) %>%
    base::unique() %>%
    dplyr::left_join(lci %>%
      dplyr::rename(
        "EIR_lci" = EIR_num,
        "lowerLLK_smooth" = smoothed_llk
      )) %>%
    dplyr::left_join(uci %>%
      dplyr::rename(
        "EIR_uci" = EIR_num,
        "upperLLK_smooth" = smoothed_llk
      )) %>%
    dplyr::mutate(
      ## If lower bound is negative, use the min_possible_EIR
      EIR_lci = base::ifelse(EIR_lci <= 0, min_possible_EIR, EIR_lci),
      ## If best EIR < lower_threshold, use the min_possible_EIR
      EIR_lci = base::ifelse(EIRmle_tilde <= lower_threshold,
        min_possible_EIR,
        EIR_lci
      ),
      ## If best EIR > upper_threshold, use the max_possible_EIR
      EIR_uci = base::ifelse(EIRmle_tilde >= upper_threshold,
        max_possible_EIR,
        EIR_uci
      ),
      ## Round the EIR values for merging  #
      EIRmle_tilde_round = base::ifelse(
        ## TODO : To be adapted to the user choice, cf. what Tatiana and Jeanne
        ##        did in their codes

        ## Round to next integer for EIRs below 10
        EIRmle_tilde <= 10, base::pmax(base::round(EIRmle_tilde), 1),
        ## Round to next even integer for EIRs above 10
        base::pmin(base::round(EIRmle_tilde / 2) * 2, max_possible_EIR)
      ),
      ## If upper bound is NA and best EIR > upper_threshold2, use the
      ## max_possible_EIR
      EIR_uci = base::ifelse(base::is.na(EIR_uci) & EIRmle_tilde_round >= upper_threshold2,
        max_possible_EIR, EIR_uci
      )
    )

  return(ci)
}


#' @title Plot quadratic approximation and smoothing
#' @description Plot smoothing and quadratic approximation of likelihood profiles
#' following Ionides et al. 2017
#' @param quadratic_l_approx Output from the function get_quadratic_fit_cf
#' @param list_setting Settings to choose from
#' @param list_sub Sub settings to choose from
#' @return A ggplot object
#' @export
plotQuadraticApprox_c <- function(quadratic_l_approx, list_setting = NULL,
                                  list_sub = NULL) {
  # Convert strings to titles
  quadratic_l_approx$setting <- str_to_title(quadratic_l_approx$setting)

  if (is.null(list_setting)) {
    list_setting <- unique(quadratic_l_approx$setting)
  }
  if (is.null(list_sub)) {
    list_sub <- unique(quadratic_l_approx$sub[which(quadratic_l_approx$setting %in% list_setting)])
  }

  plot_df <- quadratic_l_approx %>% dplyr::filter(sub %in% list_sub & setting %in% list_setting)

  g <- ggplot2::ggplot(plot_df) +
    ggplot2::geom_line(aes(x = EIR_num, y = llk_gaussian, group = seed), color = "grey68") +
    ggplot2::geom_line(aes(x = EIR_num, y = llk_tilde, color = "approx")) +
    ggplot2::geom_line(aes(x = EIR_num, y = smoothed_llk, color = "smoothed")) +
    ggplot2::geom_vline(aes(xintercept = EIRmle_tilde, color = "approx")) +
    ggplot2::facet_wrap(~setting, scales = "free") +
    ggplot2::theme_minimal() +
    ggplot2::labs(x = "EIR", y = "Gaussian likelihood", color = element_blank()) +
    ggthemes::scale_color_tableau()

  return(g)
}
