# M A I N  1 -------------------------------------------------------------------
if (FALSE)
{
  # Path to SWMM executable
  exec <- "/home/hauke/CProgramming/Stormwater-Management-Model/bin/run-swmm"

  # Set working directory to extdata directory of this package
  setwd(system.file("extdata", package = "swmmr"))
  setwd("/home/hauke/Downloads/SWMM")
  
  # List files in current directory
  dir()

  # Run SWMM on an example file
  files <- swmmr::run_swmm("Example2.inp", exec = exec)
  
  # Set path to an output file  
  out_file <- "result.out"
  out_file <- "result_example1.out"
  out_file <- files$out

  # Read metadata from the output file
  metadata <- swmmr:::read_out_metadata(out_file)

  # Start date and time
  swmmr:::times_num_to_times_posix(metadata$header$start_datetime)
  
  # Maximum number of periods
  n_periods <- metadata$footer$n_periods

  # Read the output file with the original function
  result_orig <- swmmr::read_out(out_file, 1)
  
  # Define different possible values for argument "n_parts"
  n_part_options <- 2^(seq(3, by = 2, length.out = 7))
  #n_part_options <- c(1, 2)
  
  # Define arguments to the read function
  arguments <- list(
    out_file = out_file,
    type_index = 1,
    obj_indices = 1:2,
    var_indices = 1:3,
    period_range = c(1, min(n_periods, 1000000)),
    all_in_one = FALSE,
    by_object = TRUE,
    dbg = FALSE
  )

  system.time(result <- do.call(swmmr:::read_results, c(arguments, n_parts = 1000)))
  
  lapply(result, names)
  # J5_head J5_volume J5_latflow J6_head J6_volume J6_latflow
  
  # Read the output file with the new function, with different "n_parts"
  results <- lapply(n_part_options, function(n_parts) {
    kwb.utils::catAndRun(
      messageText = paste("Reading with n_parts =", n_parts),
      newLine = 3,
      expr = do.call(swmmr:::read_results, c(arguments, n_parts = n_parts))
    )
  })
  
  # Check that the setting of "n_parts" does not influence the result
  stopifnot(kwb.utils::allAreIdentical(results))
}

# Try to read blocks of different sizes from the output file -------------------
if (FALSE)
{
  # Size of output file in bytes
  file_size <- file.size(out_file)

  # Define different possible block sizes in bytes
  blocksizes <- 2^(20:40)
  
  # Block sizes that are smaller than the file in decreasing order
  blocksizes <- rev(blocksizes[blocksizes < file_size])
  
  # Check what block sizes can be read without getting an error
  readable <- ! sapply(blocksizes, function(n) {
    kwb.utils::catAndRun(
      messageText = paste("Trying to read a block of", n / (1024^2), "MB"),
      expr = inherits(try(readBin(out_file, "raw", n)), "try-error")
    )
  })

  # Second largest readable size
  size <- blocksizes[which(readable)][2]
  
  # Size in megabytes
  (size_mb <- size / (1024^2))

  # Number of blocks of given size required to read the whole file
  n <- ceiling(file_size / size)
}
