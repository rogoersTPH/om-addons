##' @title Calibrate a country
##' @description This function is based on the methodology by Ionides et al.
##'   2017 (https://doi.org/10.1098/rsif.2017.0126) to calculate confidence
##'   intervals based on the approximate profile likelihood, accounting for
##'   Monte Carlo error. It includes a first smoothing step of the likelihood
##'   function, using the loess function. The calculation of the confidence
##'   intervals relies on a local quadratic approximation, where local refers to
##'   a selection window that can be tuned by the user with the parameters
##'   select_subset and lambda.
##' @details
##'   # Required column names
##'   Currently, 'calibrate_country()' expects the following columns names in
##'   the input data sets:
##'   simul_df: setting, year, sub, age, PR
##'   data_to_fit: setting, year, sub, age, PR_obs, sd
##' @param simul_df A dataframe containing all the OpenMalaria simulations
##'   (output of get_all_OMsimul)
##' @param data_to_fit The dataset to which the simulations are fitted
##' @param calibration_spec A list specifying the column names of the the
##'   observations and the simulations, e.g. list(cols_observed = "PR_obs",
##'   cols_simulated = "PR")
##' @param confidence Level of statistical confidence for the interval, default
##'   is 95%
##' @param lambda Proportion of the sample to be kept during the smoothing phase
##'   (default is 1)
##' @param select_subset Criterion to select only a subset of the EIR values
##'   which are near the maximum, and to select a larger sample for large EIRs
##'   and a smaller sample for small EIRs. To be adjusted by the user to get an
##'   acceptable quadratic approximation
##' @param llk_type Choose the likelihood to be used ("gaussian" or "laplace")
##' @param min_EIRnb_to_keep Minimum number of EIR values to keep, regardless of
##'   the chosen window (default is 10 and should not need to be adjusted in
##'   practice)
##' @param min_possible_EIR Lowest EIR value in the simulated grid (used when
##'   the lower bound of the confidence interval is either negative or the best
##'   EIR is below lower_threshold)
##' @param max_possible_EIR Lowest EIR value in the simulated grid (used when
##'   the best EIR is above upper_threshold)
##' @param lower_threshold Threshold for the best EIR value under which the
##'   min_possible_EIR is used as lower bound of the confidence interval
##' @param lower_threshold2 Threshold for the best EIR value above which the
##'   min_possible_EIR is used as lower bound of the confidence interval, in
##'   case the lower bound is NA
##' @param upper_threshold Threshold for the best EIR value above which the
##'   max_possible_EIR is used as upper bound of the confidence interval
##' @param upper_threshold2 Threshold for the best EIR value above which the
##'   max_possible_EIR is used as upper bound of the confidence interval, in
##'   case the upper bound is NA
##' @param inside_window if TRUE, the upper and lower bounds are found inside the window (which leads to less NA values). Default is FALSE
#'
##' @export
calibrate_country <- function(simul_df, data_to_fit,
                              calibration_spec = list(
                                cols_observed = "PR_obs",
                                cols_simulated = "PR"
                              ),
                              confidence = 0.95, lambda = 1,
                              select_subset = 0.5, llk_type = "gaussian",
                              min_EIRnb_to_keep = 10,
                              min_possible_EIR = 1,
                              max_possible_EIR,
                              lower_threshold = 3,
                              lower_threshold2 = NULL,
                              upper_threshold,
                              upper_threshold2 = NULL, 
                              inside_window=FALSE) {
  ## Make sure we have data.tables
  simul_df <- data.table::as.data.table(simul_df)
  data_to_fit <- data.table::as.data.table(data_to_fit)

  ## Initialize values
  if (is.null(lower_threshold2)) {
    lower_threshold2 <- lower_threshold
  }
  if (is.null(upper_threshold2)) {
    upper_threshold2 <- upper_threshold
  }

  ## Sanity checks
  if (any(data_to_fit$sd == 0)) {
    stop("Standard deviation in 'data_to_fit' cannot be zero.")
  }

  if (!(min_possible_EIR <= lower_threshold & min_possible_EIR <= lower_threshold2)) {
    stop("'min_possible_EIR' needs to be lower/equal than the lower thresholds.")
  }
  if (!(max_possible_EIR >= upper_threshold & max_possible_EIR >= upper_threshold2)) {
    stop("'max_possible_EIR' needs to be higher/equal than the upper thresholds.")
  }



  ## Filter EIR values below min_possible_EIR and above max_possible_EIR
  simul_df <- simul_df[EIR >= min_possible_EIR & EIR <= max_possible_EIR]

  ## Calculate likelihood or least square distance

  ## TODO Allow the user to choose between PR or Incidence (or maybe more?)
  ##      Look at "fittingVar" in munirflow, which does something similar
  ## squared_err = (PR - PR_obs)^2, # used as sanity check for llk, could
  ##                                # be removed to simplify
  ## abs_err = base::abs(PR - PR_obs), # used as sanity check for llk_AE,
  ##                                   # could be removed to simplify

  ## TODO : In the future, add Poisson and Negative binomial likelihood too
  ##        (useful for incidence)
  fitted_EIR <- data_to_fit[
    simul_df[age %in% unique(data_to_fit[["age"]])],
    ## Columns to join on
    on = c("setting", "year", "sub", "age"),
    ## These are the columns you want to keep
    .(setting, year, sub, age, seed, EIR, EIR_num,
      llk = if (llk_type == "gaussian") {
        stats::dnorm(
          get(calibration_spec[[1]]),
          mean = get(calibration_spec[[2]]),
          sd = sd,
          log = TRUE
        )
      } else {
        extraDistr::dlaplace(
          get(calibration_spec[[1]]),
          mu = get(calibration_spec[[2]]),
          sigma = sd * base::sqrt(2),
          log = TRUE
        )
      }
    )
  ]

  fitted_EIR <- fitted_EIR[, .(llk = sum(llk)),
    by = c("sub", "setting", "EIR", "EIR_num", "seed")
  ]

  ## Formating the inputs, and applying the first window parameter (select
  ## subset)

  ## We select only the relevant variables for calibration (one single year
  ## because the likelihood is already aggregated per year, and only the age to
  ## be fitted)
  profile_llk <- unique(
    ## Extract best EIR according to likelihood
    fitted_EIR[, .SD[which.max(llk)],
      by = c("sub", "setting", "seed")
    ]
  )

  ## Because it's the best EIR, we rename it and its likelihood to indicate it's
  ## the best
  data.table::setnames(profile_llk,
    old = c("EIR_num", "llk"), new = c("EIRmle", "maxLLK")
  )

  ## We merge with the full dataset containing all EIRs (not only the best)
  profile_llk <- profile_llk[fitted_EIR, on = c("sub", "setting", "seed")][
    , i.EIR := NULL
  ]
  profile_llk <- profile_llk[abs(EIR_num - EIRmle) /
    EIRmle < select_subset | abs(EIR_num - EIRmle) <= min_EIRnb_to_keep]

  ## Criterion to select the local window for quadratic fitting (because
  ## quadratic approximation is only valid locally)

  ## Calculate the smoothed likelihood with loess (includes second window
  ## parameter, lambda)
  smooth_fit <- profile_llk[,
    .(
      smoothed_llk =
        stats::predict(
          stats::loess(
            llk ~ EIR_num,
            span = lambda
          )
        ), EIR_num
    ),
    ## Grouping by districts
    by = sub
  ]
  ## Drop any NAs and only keep unique rows
  smooth_fit <- unique(na.omit(smooth_fit))

  smooth_fit <- smooth_fit[,
    {
      ## NOTE Yes, a bit annoying to write, as data.table cannot use newly
      ## created columns immediately.

      ## Calculate some summary quantities, used for the weighting in the next
      ## step. Basically, we extract/recalculate the weights from the loess
      ## fitting, to be able to incorporate them in the linear fitting

      ## Best EIR using smoothed likelihood
      smooth_arg_max <- EIR_num[which.max(smoothed_llk)]
      ## Best value of the smoothed likelihood
      smooth_max <- smoothed_llk[which.max(smoothed_llk)]
      ## Distance between the EIR and the best EIR
      dist <- abs(EIR_num - smooth_arg_max)
      ## Rank of the distance to the best EIR
      rankdist <- rank(dist)
      ## Maximum rank of the distance to the best EIR
      maxrankdist <- max(rankdist)
      ## Indicate if the EIR value is included or not based on the lambda
      ## parameter
      included <- rankdist / max(rankdist) <= lambda
      .(smoothed_llk, EIR_num,
        smooth_arg_max = smooth_arg_max,
        smooth_max = smooth_max, dist = dist, rankdist = rankdist,
        maxrankdist = maxrankdist, included = included
      )
    },
    by = sub
  ]

  ## Prepare data for fitting the quadratic linear model
  quadratic_fitprep <- smooth_fit[profile_llk, on = c("sub", "EIR_num")]
  ## Calculate the weights based on the loess fitting
  quadratic_fitprep <- quadratic_fitprep[included == TRUE,
    weight := (1 - (dist / max(dist))^3)^3,
    by = sub
  ]

  ## calculate quadratic linear model, extract the regression parameters
  ## Adapted from https://stackoverflow.com/a/13906196
  quadratic_fit_beta <- quadratic_fitprep[,
    .(model = list(lm(llk ~ EIR_num + I(EIR_num^2),
      weight = weight
    ))),
    by = sub
  ]

  quadratic_fit_beta <- quadratic_fit_beta[,
    {
      ## Only extract estimates and std error
      coefs <- summary(model[[1]])$coefficients[, 1:2]
      ## We rename the names of the terms directly here
      .(term = data.table::fifelse(
        rownames(coefs) == "(Intercept)", "c",
        data.table::fifelse(
          rownames(coefs) == "EIR_num", "b",
          data.table::fifelse(
            ## I don't think the NA case ever happens
            rownames(coefs) == "I(EIR_num^2)", "a", "NA"
          )
        )
      ), estimate = coefs[, 1], std.error = coefs[, 2])
    },
    by = sub
  ]

  ## Reformat the data
  quadratic_fit_beta <- data.table::dcast(quadratic_fit_beta,
    sub ~ term,
    value.var = c("estimate", "std.error")
  )

  ## Calculate quadratic linear model, extract the covariance
  quadratic_fit_cov <- quadratic_fitprep[,
    ## Quadratic linear fit and extract the covariance
    .(cov_ab = stats::vcov(lm(llk ~ EIR_num + I(EIR_num^2),
      weight = weight
    ))[2, 3]),
    by = sub
  ]

  ## Combine the results and calculate the delta threshold (cf. Ionides et al.
  ## 2017)
  ## First filter by included, then perform left join of x = profile_llk, y =
  ## smooth_fit and use the result in a left join with x = quadratic_fit_beta
  ## and y = result
  ## And yes, this is annoying
  Quadratic_fit <- smooth_fit[included == TRUE][
    profile_llk,
    on = c("sub", "EIR_num")
  ][quadratic_fit_beta, on = "sub"]

  Quadratic_fit <- Quadratic_fit[, `:=`(
    ## Calculate the linear approximation ax^2 +b x + c
    llk_tilde = estimate_c +
      EIR_num * estimate_b +
      EIR_num * EIR_num * estimate_a,
    ## Calculate the optimum of the linear approximation i.e. -b/2a
    EIRmle_tilde = data.table::fifelse(
      estimate_a < 0,
      -estimate_b / 2 / estimate_a,
      min(EIR_num)
    ),
    ## Calculate the arg optimum of the linear approximation, -b^2/4a +c
    mle_tilde = -(estimate_b)^2 / 4 / estimate_a + estimate_c
  )]

  Quadratic_fit <- quadratic_fit_cov[Quadratic_fit, on = "sub"]

  ## Calculate se_mc_squared and delta as in Ionides et al. 2017
  Quadratic_fit <- Quadratic_fit[
    ,
    se_mc_squared := (std.error_b^2 -
      (2 * cov_ab * estimate_b / estimate_a) +
      (estimate_b^2 * std.error_a^2 / estimate_a^2)) /
      4 /
      (estimate_a^2)
  ]
  Quadratic_fit <- Quadratic_fit[
    ,
    delta := stats::qchisq(confidence, df = 1) *
      (-estimate_a * se_mc_squared + 0.5)
  ]

  ## Drop unused columns
  drop <- c("smooth_arg_max", "dist", "rankdist", "maxrankdist", "included")
  Quadratic_fit <- Quadratic_fit[, (drop) := NULL]

  ## Input formatting, select all EIRs below the threshold
  profile_llk <- unique(Quadratic_fit[, .(EIRmle_mean = mean(EIRmle)),
    by = .(
      sub, setting, EIR_num, EIRmle_tilde,
      smoothed_llk, smooth_max, delta
    )
  ])
  
  if(inside_window){
    profile_llk <- profile_llk[smoothed_llk >= smooth_max - delta]
    
    ## Extract the upper bound (UCI=upper confidence interval)
    uci <- profile_llk[EIR_num >= EIRmle_tilde][, .SD[which.min(smoothed_llk)],
      by = c("sub", "setting")
    ][
      ,
      .(sub, setting, smoothed_llk, EIR_num)
    ]

    ## Extract the lower bound (UCI=lower confidence interval)
    lci <- profile_llk[EIR_num <= EIRmle_tilde][, .SD[which.min(smoothed_llk)],
      by = c("sub", "setting")
    ][
      ,
      .(sub, setting, smoothed_llk, EIR_num)
    ]

  } else {
    profile_llk <- profile_llk[smoothed_llk < smooth_max - delta]
    
    ## Extract the upper bound (UCI=upper confidence interval)
    uci <- profile_llk[EIR_num >= EIRmle_tilde][, .SD[which.max(smoothed_llk)],
      by = c("sub", "setting")
    ][
      ,
      .(sub, setting, smoothed_llk, EIR_num)
    ]

    ## Extract the lower bound (UCI=lower confidence interval)
    lci <- profile_llk[EIR_num <= EIRmle_tilde][, .SD[which.max(smoothed_llk)],
      by = c("sub", "setting")
    ][
      ,
      .(sub, setting, smoothed_llk, EIR_num)
    ]

  }


  # Combine all to get the confidence interval, and add ifelse conditions on the
  # edge of the grid
  ci <- unique(Quadratic_fit[, .(EIRmle_mean = mean(EIRmle)),
    by = .(sub, setting, EIRmle_tilde, smooth_max)
  ])
  ci <- lci[ci, on = c("sub", "setting")]
  data.table::setnames(ci,
    old = c("EIR_num", "smoothed_llk"), new = c("EIR_lci", "lowerLLK_smooth")
  )
  ci <- uci[ci, on = c("sub", "setting")]
  data.table::setnames(ci,
    old = c("EIR_num", "smoothed_llk"), new = c("EIR_uci", "upperLLK_smooth")
  )
  ci <- ci[, `:=`(
    ## If lower bound is negative, use the min_possible_EIR
    EIR_lci = data.table::fifelse(EIR_lci <= 0, min_possible_EIR, EIR_lci)
  )]

  ci <- ci[, `:=`(
    ## If best EIR < lower_threshold, use the min_possible_EIR
    EIR_lci = data.table::fifelse(
      EIRmle_tilde <= lower_threshold,
      min_possible_EIR,
      EIR_lci
    ),
    ## If best EIR > upper_threshold, use the max_possible_EIR
    EIR_uci = data.table::fifelse(
      EIRmle_tilde >= upper_threshold,
      max_possible_EIR,
      EIR_uci
    ),
    ## Round the EIR values for merging  #
    EIR = data.table::fifelse(
      ## TODO : To be adapted to the user choice, cf. what Tatiana and Jeanne
      ##        did in their codes

      ## Round to next integer for EIRs below 10
      EIRmle_tilde <= 10, pmax(base::round(EIRmle_tilde), 1),
      ## Round to next even integer for EIRs above 10
      pmin(round(EIRmle_tilde / 2) * 2, max_possible_EIR)
    )
  )]

  ci <- ci[, `:=`(
    ## If upper bound is NA and best EIR > upper_threshold2, use the
    ## max_possible_EIR
    EIR_uci = data.table::fifelse(
      is.na(EIR_uci) & EIR >= upper_threshold2,
      max_possible_EIR, EIR_uci
    )
  )]

  ## Cleanup columns
  drop <- c("upper_LLK_smooth", "lower_LLK_smooth")
  ci <- ci[, (drop) := NULL]

  return(list(
    "quadratic_fit" = Quadratic_fit,
    "best_EIRs_ci" = ci
  ))
}
