##' @title Add sublist to list
##' @param data List to add to.
##' @param sublist Sublist, under which the entry should get added. Must be
##'   given as a vector, e.g. c("level1", "level2", ...). Can be NULL.
##' @param entry Name of the entry to add, as string. Can be NULL.
##' @param input List to add.
##' @param append Boolean. Determines if the list should be appended to existing
##'   entries or overwrite them.
##' @keywords internal
.xmlAddList <- function(data, sublist, entry, input, append = TRUE) {
  ## If we don't want to append, we will override the present entry.
  if (append == FALSE) {
    ## Make sure to remove all entries corresponding to sublist
    if (!is.null(sublist)) {
      data[[c(sublist)]] <- data[[c(sublist)]][
        names(data[[c(sublist)]]) %in% c(entry) == FALSE
      ]
    }
    ## Add new entry
    if (!is.null(entry)) {
      data[[c(sublist, entry)]] <- input
    } else {
      data <- input
    }
    return(data)
  } else {
    ## Otherwise we append the data
    ## If sublist is not given, backup whole list
    if (!is.null(sublist)) {
      oldEntry <- data[[sublist]]
    } else {
      oldEntry <- data
    }
    ## Create new list
    newEntry <- list()
    if (!is.null(entry)) {
      newEntry[[entry]] <- input
    } else {
      newEntry <- input
    }

    ## Append to old list
    if (!is.null(sublist)) {
      data[[sublist]] <- append(oldEntry, newEntry)
    } else {
      data <- append(oldEntry, newEntry)
    }
    return(data)
  }
}


##' @title Make sure required elements are declared
##' @param baseList List with experiment data.
##' @param name Name of the interventions.
##' @keywords internal
.defineInterventionsHeader <- function(baseList, name = "All interventions") {
  ## Verify input
  assertCol <- checkmate::makeAssertCollection()
  checkmate::assertList(baseList, add = assertCol)
  checkmate::assertCharacter(name, add = assertCol)
  checkmate::reportAssertions(assertCol)

  baseList <- .xmlAddList(
    data = baseList,
    sublist = "interventions",
    entry = "name",
    input = name,
    append = FALSE
  )

  return(baseList)
}


##' @title Reads base xml file and creates list to be edited with
##' openMalariaUtilities
##' @param filename Name of OpenMalaria base xml file
##' @export

baseXmlToList <- function(filename) {
  ## Remove reserved attributes specific to R
  remove_reserved <- function(this_attr) {
    reserved_attr <- c("class", "comment", "dim", "dimnames", "names", "row.names", "tsp")
    if (!any(reserved_attr %in% names(this_attr))) {
      return(this_attr)
    }
    for (reserved in reserved_attr) {
      if (!is.null(this_attr[[reserved]])) this_attr[[reserved]] <- NULL
    }
    this_attr
  }


  ## Promote attribute
  promote_attr <- function(ll) {
    this_attr <- attributes(ll)
    this_attr <- remove_reserved(this_attr)
    if (is.list(ll)) {
      # recursive case, some exceptions
      c(this_attr, ll %>%
        purrr::imap(~ if (!.y %in% c("value", "surveyTime")) {
          .x %>% promote_attr()
        } else {
          .x
        }))
    } else {
      # base case (no sub-items)
      this_attr
    }
  }

  ## Read xml file and parse as list and promote attributes
  outList <- file.path(filename) %>%
    xml2::read_xml(.) %>%
    xml2::as_list(.) %>%
    .[["scenario"]] %>%
    promote_attr() %>%
    rlist::list.clean(., recursive = T)

  ## Add list items that are mandatory for openMalariaUtilities
  outList$OMVersion <- as.integer(outList$schemaVersion)

  if (!is.null(outList$name)) {
    outList$expName <- outList$name
  } else {
    warning("You need to provide a name for $expName before using
            openMalariaUtilities")
  }
  if (is.null(outList$monitoring$startDate)) {
    warning("You need to provide a start date ('YYYY-MM-DD') for $monitoring$startDate
            before using openMalariaUtilities")
  }


  return(outList)
}
