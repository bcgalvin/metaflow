#' R interface to Metaflow
#'
#' Interface to 'Metaflow' <https://metaflow.org/>, a human-friendly 
#' Python framework for building and managing real-life data science projects. 
#' Originally developed at Netflix, Metaflow offers a unified API for data 
#' science workflows, covering the full development lifecycle of a data science project.
#' 
#' The Metaflow package allows data scientists using R to leverage Metaflow's scalability and reproducibility 
#' all while staying within their preferred ecosystem.
#'
#' See the package website at <https://docs.metaflow.org> for complete documentation.
#'
#' @importFrom reticulate
#'   import py_install virtualenv_create py_to_r r_to_py
#' @aliases metaflow-package
"_PACKAGE"
