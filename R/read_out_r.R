# M A I N ----------------------------------------------------------------------
if (FALSE)
{
  out_file <- "/home/hauke/Downloads/SWMM/result.out"

  read_out_metadata(out_file)
}

# field_config -----------------------------------------------------------------
field_config <- list(
  meta_header = list(
    magic_number = "int",
    version = "int",
    flow_units = "int"
  ),
  counts = list(
    subcatch = "int", 
    nodes = "int", 
    links = "int",
    polluts = "int"
  ),
  subcatch = list(
    area = "num"
  ),
  node = list(
    node_type = "int", 
    invert_elevation = "num", 
    full_depth = "num"
  ),
  link = list(
    link_type = "int",
    offset_1 = "num",
    offset_2 = "num",
    max_depth = "num",
    length = "num"
  )
)

# read_out_metadata ------------------------------------------------------------
read_out_metadata <- function(out_file)
{
  # Provide read-functions
  read_functions <- lapply(field_config, function(x) {
    do.call(get_read_function, x)
  })
  
  # Open output file
  con <- file(out_file, "rb")
  
  # Close output file on exit
  on.exit(close(con))
  
  # Initialise list of offsets
  offsets <- list()

  # Read first header fields
  header <- read_functions$meta_header(1, con)
  
  # Read numbers of subcatchments, nodes, links, pollutants
  object_counts <- read_functions$counts(1, con)
  
  # Read names of subcatchments, nodes, links
  object_names <- lapply(object_counts, read_names, con)

  # Read pollution units  
  pollution_units <- read_int(con, object_counts$polluts)
  
  # Save current file pointer offset
  offsets$input_start <- seek(con, where = NA)

  # Read subcatchment properties
  subcatch <- read_objects(
    n = length(field_config$subcatch), 
    con = con,
    ids = object_names$subcatch, 
    fun = read_functions$subcatch
  )

  # Read node properties
  nodes <- read_objects(
    n = length(field_config$node), 
    con = con,
    ids = object_names$nodes, 
    fun = read_functions$node
  )

  # Read link properties
  links <- read_objects(
    n = length(field_config$link), 
    con = con,
    ids = object_names$links, 
    fun = read_functions$link
  )
  
  metadata <- list(
    header = header,
    subcatch = subcatch,
    nodes = nodes,
    links = links
  )
  
  metadata
}

# get_read_function ------------------------------------------------------------
get_read_function <- function(...) {
  
  function(i, con) {
    as.data.frame(stringsAsFactors = FALSE, lapply(list(...), function(x) {
      (list(int = read_int, num = read_num)[[x]])(con)
    }))
  }
}

# read_int ---------------------------------------------------------------------
read_int <- function(con, n = 1) readBin(con, "integer", n)

# read_num ---------------------------------------------------------------------
read_num <- function(con, n = 1) readBin(con, "numeric", n, size = 4)

# read_names -------------------------------------------------------------------
read_names <- function(n, con) {
  
  sapply(seq_len(n), function(i) readChar(con, readBin(con, "integer")))
}

# read_objects -----------------------------------------------------------------
read_objects <- function(n, con, ids, fun) {
  
  stopifnot(identical(read_int(con), n))
  
  read_int(con, n)
  
  data <- do.call(rbind, lapply(ids, fun, con))
  
  data.frame(name = ids, data, stringsAsFactors = FALSE)
}

# for (i in seq_len(n)) {
#   cat(sprintf("Reading block %d/%d\n", i, n))
#   raw_bytes <- readBin(con, what = "raw", n = size)
#   print(length(raw_bytes))
#   print(head(raw_bytes))
#   print(tail(raw_bytes))
# }
# 
# 
# system.time(x <- readBin(out_file, what = "raw", n = n))# 
# 
# INT4 NumSubcatchVars
# 01 INT4 SUBCATCH_RAINFALL
# 02 INT4 SUBCATCH_SNOWDEPTH
# 03 INT4 SUBCATCH_EVAP
# 04 INT4 SUBCATCH_INFIL
# 05 INT4 SUBCATCH_RUNOFF
# 06 INT4 SUBCATCH_GW_FLOW
# 07 INT4 SUBCATCH_GW_ELEV
# 08 INT4 SUBCATCH_SOIL_MOIST
# NumPolluts * (
#   INT4 SUBCATCH_WASHOFF + j
# )
# 
# INT4 NumNodeVars
# 01 INT4 NODE_DEPTH
# 02 INT4 NODE_HEAD
# 03 INT4 NODE_VOLUME
# 04 INT4 NODE_LATFLOW
# 05 INT4 NODE_INFLOW
# 06 INT4 NODE_OVERFLOW
# NumPolluts * (
#   INT4 NODE_QUAL + j
# )
# 
# INT4 NumLinkVars
# 01 INT4 LINK_FLOW
# 02 INT4 LINK_DEPTH
# 03 INT4 LINK_VELOCITY
# 04 INT4 LINK_VOLUME
# 05 INT4 LINK_CAPACITY
# NumPolluts * (
#   INT4 LINK_QUAL + j
# )
# 
# INT4 MAX_SYS_RESULTS
# MAX_SYS_RESULTS * INT4 (values 0 to MAX_SYS_RESULTS - 1)
# 
# REAL8 StartDateTime (complicated calculation)
# INT4 ReportStep
# 
# 
# 
# file_size <- file.size(out_file)
# 
# blocksizes <- 2^(10:30)
# blocksizes <- rev(blocksizes[blocksizes < file_size])
# 
# readable <- sapply(blocksizes, function(n) {
#   print(n)
#   ! inherits(try(readBin(out_file, "raw", n)), "try-error")
# })
# 
# size <- blocksizes[which(readable)][2]
# size_mb <- size/(1024^2)
# 
# n <- file_size %/% size
# 
