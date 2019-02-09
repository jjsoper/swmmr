if (FALSE) {

  show_selection(
    config, 
    indices = get_column_index(config, iType = 0, obj_indices = 0, var_indices = 0)
  )
}

# Define fake configuration ----------------------------------------------------
config <- list(
  catch = list(
    iType = 0,
    n_objects = 3,
    n_vars = 3,
    obj_indices = c(0,1,2),
    var_indices = 0:2
  ),
  nodes = list(
    iType = 1,
    n_objects = 2,
    n_vars = 4,
    obj_indices = c(0,1),
    var_indices = c(0, 3)
  ), 
  links = list(
    iType = 2,
    n_objects = 4,
    n_vars = 2,
    obj_indices = c(0,1,2,3),
    var_indices = 0:1
  )
)

# get_column_index -------------------------------------------------------------
get_column_index <- function(config, iType, obj_indices, var_indices) {
  offsets <- get_offsets(get_blocksizes(config))
  conf <- config[[iType + 1]]
  offsets[[iType + 1]] + 
    rep(var_indices, length(obj_indices)) +
    rep(obj_indices, each = length(var_indices)) * conf$n_vars + 1
}

# get_blocksizes ---------------------------------------------------------------
get_blocksizes <- function(config) {
  sapply(config, function(x) x$n_objects * x$n_vars)
}

# get_offsets ------------------------------------------------------------------
get_offsets <- function(blocksizes) {
  names_backup <- names(blocksizes)
  blocksizes <- get_blocksizes(config)
  offsets <- c(0, cumsum(blocksizes)[-length(blocksizes)])
  stats::setNames(offsets, names_backup)
}

# show_selection ---------------------------------------------------------------
show_selection <- function(config, indices) {
  demo_matrix <- get_demo_matrix(config)
  demo_matrix[indices] <- "x"
  demo_matrix
}

# get_demo_matrix --------------------------------------------------------------
get_demo_matrix <- function(config) {
  #name <- names(config)[2]
  columns <- unlist(sapply(names(config), function(name) {
    n_vars <- config[[name]]$n_vars
    n_objs <- config[[name]]$n_objects
    id <- substr(name, 1, 1)
    obj_name <- sprintf("%s%d", id, rep(seq_len(n_objs) - 1, each = n_vars))
    var_name <- rep(sprintf("v%d", seq_len(n_vars) - 1), n_objs)
    paste0(obj_name, var_name)
  }))
  
  as.data.frame(stringsAsFactors = FALSE, matrix(
    "", nrow = 1, ncol = length(columns), dimnames = list(NULL, columns)
  ))
}
