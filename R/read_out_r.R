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
  ),
  
  subcatch_vars = list(
    rainfall = "int",
    snowdepth = "int",
    evap = "int",
    infil = "int",
    runoff = "int",
    gw_flow = "int",
    gw_elev = "int",
    soil_moist = "int"
  ),
  
  node_vars = list(
    depth = "int",
    head = "int",
    volumne = "int",
    latflow = "int",
    inflow = "int",
    overflow = "int"
  ),
  
  link_vars = list(
    flow = "int",
    depth = "int",
    velocity = "int",
    volume = "int",
    capacity = "int"
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
  header <- read_functions$meta_header(con, 1)
  
  # Read numbers of subcatchments, nodes, links, pollutants
  object_counts <- read_functions$counts(con, 1)
  
  # Read names of subcatchments, nodes, links
  object_names <- lapply(object_counts, read_names, con = con)

  # Read pollution units  
  pollution_units <- read_int(con, object_counts$polluts)
  
  # Save current file pointer offset
  offsets$input_start <- seek(con, where = NA)

  # Read subcatchment properties
  subcatch <- read_objects(
    con,
    n = length(field_config$subcatch), 
    ids = object_names$subcatch, 
    fun = read_functions$subcatch
  )

  # Read node properties
  nodes <- read_objects(
    con,
    n = length(field_config$node), 
    ids = object_names$nodes, 
    fun = read_functions$node
  )

  # Read link properties
  links <- read_objects(
    con,
    n = length(field_config$link), 
    ids = object_names$links, 
    fun = read_functions$link
  )

  # Read subcatchment variables
  subcatch_vars <- read_object_vars(
    con,
    n = length(field_config$subcatch_vars),
    read_function = read_functions$subcatch_vars, 
    n_polluts = object_counts$polluts
  )
  
  # Read node variables
  node_vars <- read_object_vars(
    con,
    n = length(field_config$node_vars),
    read_function = read_functions$node_vars, 
    n_polluts = object_counts$polluts
  )

  # Read link variables
  link_vars <- read_object_vars(
    con,
    n = length(field_config$link_vars),
    read_function = read_functions$link_vars, 
    n_polluts = object_counts$polluts
  )

  metadata <- list(
    header = header,
    subcatch = subcatch,
    nodes = nodes,
    links = links,
    subcatch_vars = subcatch_vars,
    node_vars = node_vars,
    link_vars = link_vars
  )
  
  metadata
}

# get_read_function ------------------------------------------------------------
get_read_function <- function(...) {
  
  function(con, i) {
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
read_names <- function(con, n) {
  
  sapply(seq_len(n), function(i) readChar(con, readBin(con, "integer")))
}

# read_objects -----------------------------------------------------------------
read_objects <- function(con, n, ids, fun) {
  
  stopifnot(identical(read_int(con), n))
  read_int(con, n)
  data <- do.call(rbind, lapply(ids, fun, con = con))
  data.frame(name = ids, data, stringsAsFactors = FALSE)
}

# read_object_vars -------------------------------------------------------------
read_object_vars <- function(con, n, read_function, n_polluts) {
  
  stopifnot(identical(read_int(con), n))
  result <- read_function(con, 1)
  read_int(con, n_polluts)
  result
}

# for (i in seq_len(n)) {
#   cat(sprintf("Reading block %d/%d\n", i, n))
#   raw_bytes <- readBin(con, what = "raw", n = size)
#   print(length(raw_bytes))
#   print(head(raw_bytes))
#   print(tail(raw_bytes))
# }
# 
# system.time(x <- readBin(out_file, what = "raw", n = n))# 
# 
# INT4 MAX_SYS_RESULTS
# MAX_SYS_RESULTS * INT4 (values 0 to MAX_SYS_RESULTS - 1)
# 
# REAL8 StartDateTime (complicated calculation)
# INT4 ReportStep
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
