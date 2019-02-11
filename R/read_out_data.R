# read_results -----------------------------------------------------------------
read_results <- function(
  out_file, type_index, obj_indices, var_indices, period_range, n_parts = NULL,
  metadata = NULL, all_in_one = FALSE, by_object = FALSE, blocksize_mb = 128,
  dbg = TRUE
)
{
  if (FALSE) {
    metadata <- NULL; type_index = 1; obj_indices = 0:2; var_indices = 0:2
    period_range = c(1, 20001); n_parts = 2; dbg = FALSE
  }
  
  if (is.null(metadata)) {
    
    metadata <- read_out_metadata(out_file)
  }
  
  # Find a good value for n_parts if no value is given
  if (is.null(n_parts)) {
    
    n_parts <- swmmr:::find_good_number_of_parts(
      file_size = file.size(out_file), 
      metadata = metadata, 
      period_range = period_range,
      blocksize_mb = blocksize_mb
    )
  }
  
  # Determine start and end period for each of the n_parts blocks to read
  block_ranges <- get_block_ranges(period_range, n_parts = n_parts)
  
  # Number of blocks to read
  n_blocks <- nrow(block_ranges)
  
  # Number of periods to read in each block
  n_periods <- block_ranges[, 2] - block_ranges[, 1] + 1
  
  # Determine the columns corresponding to the selected objects and variables  
  columns <- get_column_index(metadata, type_index, obj_indices, var_indices)
  
  # Keep also the first two columns containing the timestamp
  columns <- c(1, 2, columns)
  
  # Spread column indices to span 4 columns each (4 bytes per value)
  columns <- spread_indices(columns, width = 4)
  
  # Determine the number of numeric values (4 bytes each) stored for one period
  n_values_per_period <- sum(metadata$block_sizes) + 2 # 8 bytes for report time
  
  # Open the output file  
  con <- file(out_file, "rb")
  
  # Close the output file on exit
  on.exit(close(con))
  
  # Determine the start position in the output file  
  offset <- metadata$offsets$output_start
  
  if (period_range[1] > 1) {
    
    offset <- offset + (period_range[1] - 1) * 4 * n_values_per_period
  }
  
  # Set the file pointer in the output file  
  seek(con, offset)
  
  # Read the blocks in a loop
  result_list <- lapply(seq_len(n_blocks), function(i) {
    
    kwb.utils::catIf(dbg, sprintf("%3d ", i))
    
    n <- n_periods[i]
    
    matrix(ncol = n, data = readBin(
      con, "raw", n * n_values_per_period * 4
    ))[columns, , drop = FALSE]
  })
  
  # Merge the blocks
  result <- do.call(cbind, result_list)
  
  # Convert matrix to xts table
  xts_matrix <- convert_byte_matrix_to_xts(result)
  
  # Check if there are as many columns as total variables were requested 
  stopifnot(ncol(xts_matrix) == length(obj_indices) * length(var_indices))

  if (all_in_one) {
    # Get column names according to object and variable indices  
    columns <- get_variable_names(metadata, type_index, obj_indices, var_indices)
    
    # Set column names  
    dimnames(xts_matrix) <- list(NULL, columns)
    
    # Return xts table
    xts_matrix
    
  } else {
    
    split_by_object_or_variable(
      xts_matrix, metadata, type_index, obj_indices, var_indices, by_object
    )
  }
}

# find_good_number_of_parts ----------------------------------------------------
find_good_number_of_parts <- function(
  file_size, metadata, period_range, blocksize_mb = 128
)
{
  n_periods <- metadata$footer$n_periods
  bytes_per_period <- (8 + 4 * sum(metadata$block_sizes))
  
  if (! identical(
    bytes_per_period * n_periods, 
    file_size - metadata$offsets$output_start - 6 *4
  )) {
    
    stop(
      "The calculated number of bytes in the file does not correspond to ", 
      "the file size minus header and footer bytes", call. = FALSE
    )
  }

  bytes_requested <- (diff(period_range) + 1) * bytes_per_period
  
  # At least one part
  n_parts <- max(1, round((bytes_requested / 1024^2) / blocksize_mb))
  
  message(sprintf(
    "The file will be read in %d parts of about %d MB each", 
    n_parts, round(blocksize_mb)
  ))
  
  n_parts
}

# get_block_ranges -------------------------------------------------------------
get_block_ranges <- function(period_range, n_parts)
{
  # Size of one part
  part_size <- (period_range[2] - period_range[1] + 1) / n_parts

  # Start indices of blocks
  i_from <- period_range[1] + c(0L, round(part_size * seq_len(n_parts - 1)))
  
  # End indices of blocks
  i_to <- c(i_from[-1] - 1, period_range[2])

  # Combine start and end indices  
  cbind(i_from, i_to)
}

# get_column_index -------------------------------------------------------------
get_column_index <- function(metadata, type_index, obj_indices, var_indices)
{
  offsets <- get_offsets(metadata$block_sizes)
  
  n_variables_per_type <- list(
    length(metadata$subcatch_vars),
    length(metadata$node_vars), 
    length(metadata$link_vars),
    length(metadata$sys_result_ids)
  )
  
  n_vars <- n_variables_per_type[[type_index + 1]]
  
  offsets[[type_index + 1]] + 
    rep(var_indices, length(obj_indices)) +
    rep(obj_indices, each = length(var_indices)) * n_vars + 3
}

# get_offsets ------------------------------------------------------------------
get_offsets <- function(blocksizes)
{
  element_names <- names(blocksizes)
  
  offsets <- c(0, cumsum(blocksizes)[-length(blocksizes)])
  
  stats::setNames(offsets, element_names)
}

# spread_indices ---------------------------------------------------------------
spread_indices <- function(indices, width)
{
  width * rep(indices - 1, each = width) + seq_len(width)
}

# convert_byte_matrix_to_xts ---------------------------------------------------
convert_byte_matrix_to_xts <- function(byte_matrix)
{
  # We expect a matrix of bytes (raw)
  stopifnot(is.matrix(byte_matrix), is.raw(byte_matrix))
  
  # Create the timestamps from the first eight rows
  times_posix <- times_num_to_times_posix(extract_times_numeric(byte_matrix))
  
  # Calculate number of numeric values to read
  n_numerics <- (length(byte_matrix) - 8 * ncol(byte_matrix)) / 4
  
  # Create 4-byte numerics from the remaining rows
  numerics <- readBin(byte_matrix[-(1:8), ], "numeric", n_numerics, size = 4)
  
  # Check if as many values as requested could be read
  stopifnot(n_numerics == length(numerics))
  
  # Arrange values in a matrix with one row per period
  value_matrix <- matrix(numerics, byrow = TRUE, nrow = ncol(byte_matrix))
  
  # Create xts matrix
  xts::xts(value_matrix, order.by = times_posix)
}

# extract_times_numeric --------------------------------------------------------
extract_times_numeric <- function(raw_matrix)
{
  stopifnot(is.matrix(raw_matrix), is.raw(raw_matrix))
  
  readBin(raw_matrix[1:8, ], "numeric", n = ncol(raw_matrix), size = 8)
}

# times_num_to_times_posix -----------------------------------------------------
times_num_to_times_posix <- function(x, tz = "UTC")
{
  as.POSIXct(x * 86400, origin = "1899-12-30", tz = tz)
}

# get_variable_names -----------------------------------------------------------
get_variable_names <- function(metadata, type_index, obj_indices, var_indices)
{
  names_list <- indices_to_names(metadata, type_index, obj_indices, var_indices)
  
  sprintf(
    "%s_%s", 
    rep(names_list$objects, each = length(var_indices)), 
    rep(names_list$variables, length(obj_indices))
  )
}

# indices_to_names -------------------------------------------------------------
indices_to_names <- function(metadata, type_index, obj_indices, var_indices)
{
  if (type_index == 3) {
    
    object_names <- "system_variable"
    variable_names <- get_system_variable_names()
    
  } else {
    
    element <- c("subcatch", "nodes", "links")[type_index + 1]
    object_names <- metadata[[element]]$name
    
    element <- c("subcatch_vars", "node_vars", "link_vars")[type_index + 1]
    variable_names <- names(metadata[[element]])
  }

  list(
    objects = object_names[obj_indices + 1], 
    variables = variable_names[var_indices + 1]
  )  
}

# get_system_variable_names ----------------------------------------------------
get_system_variable_names <- function()
{
  c("air_temperature",             #  0
    "total_rainfall",              #  1
    "total_snow_depth",            #  2
    "average_losses",              #  3
    "total_runoff",                #  4
    "total_dry_weather_inflow",    #  5
    "total_groundwater_inflow",    #  6
    "total_RDII_inflow",           #  7
    "total_external_inflow",       #  8
    "total_direct_inflow",         #  9
    "total_external_flooding",     # 10
    "total_outflow_from_outfalls", # 11
    "total_nodal_storage_volume",  # 12
    "potential_evaporation",       # 13
    "actual_evaporation"           # 14
  )
}

# split_by_object_or_variable --------------------------------------------------
split_by_object_or_variable <- function(
  xts_matrix, metadata, type_index, obj_indices, var_indices, by_object
)
{
  names_list <- indices_to_names(metadata, type_index, obj_indices, var_indices)

  #names_list <- swmmr:::indices_to_names(metadata, 0, 0:2, 0:3)
  (names_lengths <- lengths(names_list))
  
  # The elements in names_list are 1. object_names, 2. variable_names
  # Create a helper matrix containing the column indices. Each column represents
  # an object and each row represents a variable
  index_matrix <- array(
    seq_len(prod(names_lengths)), 
    dim = rev(names_lengths), 
    dimnames = rev(names_list)
  )

  (indices <- seq_along(names_list[[ifelse(by_object, 1, 2)]]))
    
  stats::setNames(
    object = lapply(indices, function(i) {
      columns <- if (by_object) {
        index_matrix[, i]
      } else {
        index_matrix[i, ]
      }
      result <- xts_matrix[, columns]
      colnames(result) <- names_list[[ifelse(by_object, 2, 1)]]
      result
    }), 
    nm = names_list[[ifelse(by_object, 1, 2)]]
  )
}
