#' Helper utility to serialize R object to metaflow
#' data format
#'
#' @param object object to serialize
#' @return metaflow data format object
mf_serialize <- function(object) {
  qs::qserialize(object)
}

#' Helper utility to deserialize objects from metaflow
#' data format to R object
#'
#' @param object object to deserialize
#' @return R object
mf_deserialize <- function(object) {
  if (is.raw(object)) {
    qs::qdeserialize(object)
  } else {
    object
  }
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#'
#' @section Usage:
#' \preformatted{
#'  print(self$var)
#' }
#' @export
"$.metaflow.flowspec.FlowSpec" <- function(self, name) {
  value <- NextMethod(name)
  mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self$var <- "hello"
#' }
#' @export
"$<-.metaflow.flowspec.FlowSpec" <- function(self, name, value) {
  value <- mf_serialize(value)
  NextMethod(name, value)
}

#' Overload getter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#'
#' @section Usage:
#' \preformatted{
#'  print(self[["var"]])
#' }
#' @export
"[[.metaflow.flowspec.FlowSpec" <- function(self, name) {
  value <- NextMethod(name)
  mf_deserialize(value)
}

#' Overload setter for self object
#'
#' @param self the metaflow self object for each step function
#' @param name attribute name
#' @param value value to assign to the attribute
#'
#' @section Usage:
#' \preformatted{
#'  self[["var"]] <- "hello"
#' }
#' @export
"[[<-.metaflow.flowspec.FlowSpec" <- function(self, name, value) {
  value <- mf_serialize(value)
  NextMethod(name, value)
}

#' Helper utility to gather inputs in a join step
#'
#' @param inputs inputs from parent branches
#' @param input field to extract from inputs from
#' parent branches into vector
#' @section usage:
#' \preformatted{
#' gather_inputs(inputs, "alpha")
#' }
#' @export
gather_inputs <- function(inputs, input) {
  lapply(seq_along(inputs), function(x) {
    inputs[[x]][[input]]
  })
}

#' Helper utility to merge artifacts in a join step
#'
#' @param flow flow object
#' @param inputs inputs from parent branches
#' @param exclude list of artifact names to exclude from merging
#' @examples
#' \dontrun{
#' merge_artifacts(flow, inputs)
#' }
#' \dontrun{
#' merge_artifacts(flow, inputs, list("alpha"))
#' }
#' @export
merge_artifacts <- function(flow, inputs, exclude = list()) {
  flow$merge_artifacts(unname(inputs), exclude)
}

#' Helper utility to access current IDs of interest
#'
#' @param value one of flow_name, run_id, origin_run_id,
#'              step_name, task_id, pathspec, namespace,
#'              username, retry_count
#' @examples
#' \dontrun{
#' current("flow_name")
#' }
#' @export
current <- function(value) {
  pkg.env$mf$current[[value]]
}

escape_bool <- function(x) {
  ifelse(x, "True", "False")
}

escape_quote <- function(x) {
  if (x %in% c("TRUE", "FALSE")) {
    ifelse(x == "TRUE", "True", "False")
  } else {
    encodeString(x, quote = "'")
  }
}

space <- function(len, type = "h") {
  switch(type,
    "h" = strrep(" ", len),
    "v" = strrep("\n", len)
  )
}

wrap_argument <- function(x) {
  x <- x[[1]]
  if (is.character(x)) {
    x <- escape_quote(x)
  }
  if (is.logical(x)) {
    x <- escape_bool(x)
  }
  x
}

#' Return installation path of metaflow R library
#' @param flowRDS path of the RDS file containing the flow object
#' @export
metaflow_location <- function(flowRDS) {
  list(
    package = system.file(package = "metaflow"),
    flow = suppressWarnings(normalizePath(flowRDS)),
    wd = suppressWarnings(normalizePath(paste0(getwd())))
  )
}

extract_ids <- function(obj) {
  extract_str <- function(x) {
    chr <- as.character(x)
    gsub("'", "", regmatches(chr, gregexpr("'([^']*)'", chr))[[1]])
  }
  unlist(lapply(
    reticulate::import_builtins()$list(obj),
    function(x) {
      sub(".*/", "", extract_str(x))
    }
  ))
}

extract_str <- function(x) {
  chr <- as.character(x)
  gsub("'", "", regmatches(chr, gregexpr("'([^']*)'", chr))[[1]])
}

#' Return a vector of all flow ids.
#'
#' @export
list_flows <- function() {
  pkg.env$mf$Metaflow()$flows %>%
    extract_ids()
}

test_helloworld_flow<- function(){
  start <- function(self) {
    print("Your Metaflow installation looks good!")
  }

  metaflow("HelloWorldFlow") %>%
    step(
      step = "start",
      r_function = start,
      next_step = "end"
    ) %>%
    step(
      step = "end"
    ) %>%
    run()
}

#' Run a test to check if Metaflow R is installed properly
#'
#' @export
test <- function() {
  if (!pkg.env$activated || !check_python_dependencies()){
    print_metaflow_install_options()
  } else {
    test_helloworld_flow()
  }
}

#' Return Metaflow python version
py_version <- function() {
  version <- pkg.env$mf$metaflow_version$get_version()
  c(python_version = version)
}

#' Return Metaflow R version
#' @export
r_version <- function() {
  # utils library usually comes with the standard installation of R
  version <- as.character(unclass(utils::packageVersion("metaflow"))[[1]])
  if (length(version) > 3) {
    version[4:length(version)] <- as.character(version[4:length(version)])
  }
  paste0(version, collapse = ".")
}

#' Return the default container image to use for remote execution on AWS Batch.
#' By default we user docker images maintained on https://hub.docker.com/r/rocker/ml.
#'
#' @export
container_image <- function() {
  rocker_image_tags <- c(
    "3.5.2", "3.5.3", "3.6.0",
    "3.6.1", "4.0.0", "4.0.1", "4.0.2"
  )

  local_r_version <- paste(R.version$major, R.version$minor, sep = ".")

  rocker_tag <- local_r_version
  if (!local_r_version %in% rocker_image_tags) {
    version_split <- strsplit(local_r_version, split = "[.]")[[1]]
    r_version <- paste(version_split[1], version_split[2], sep = ".")

    # if there's no exact match, find the best match of R versions.
    if (r_version < "3.5") {
      rocker_tag <- "3.5.2"
    } else if (r_version == "3.5") {
      rocker_tag <- "3.5.3"
    } else if (r_version == "3.6") {
      rocker_tag <- "3.6.1"
    } else if (r_version == "4.0") {
      rocker_tag <- "4.0.2"
    } else {
      rocker_tag <- "latest"
    }
  }

  return(paste0("rocker/ml:", rocker_tag))
}

#' Pull the R tutorials to the current folder
#' @export
pull_tutorials <- function(){
  tutorials_folder <- system.file("tutorials", package = "metaflow")
  file.copy(tutorials_folder, ".", recursive=TRUE)
  invisible()
}

#' Print out Metaflow version
#' @export
version_info <- function(){
  message(sprintf("Metaflow (R) %s", r_version()))
  message(sprintf("Metaflow (Python) %s", py_version()))

  invisible()
}