# read_out_metadata ------------------------------------------------------------
read_out_metadata <- function(out_file)
{
  # Provide configuration
  field_config <- get_metadata_field_config()
  
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
  header <- read_functions$header(con, 1)
  
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
  
  # Provide number of pollutants
  n_polluts <- object_counts$polluts
  
  # Read subcatchment variables
  subcatch_vars <- read_object_vars(
    con,
    n = length(field_config$subcatch_vars),
    read_function = read_functions$subcatch_vars, 
    n_polluts = n_polluts
  )
  
  # Read node variables
  node_vars <- read_object_vars(
    con,
    n = length(field_config$node_vars),
    read_function = read_functions$node_vars, 
    n_polluts = n_polluts
  )
  
  # Read link variables
  link_vars <- read_object_vars(
    con,
    n = length(field_config$link_vars),
    read_function = read_functions$link_vars, 
    n_polluts = n_polluts
  )
  
  # Read system variable count and "ids"
  max_sys_results <- read_int(con)
  sys_result_ids <- read_int(con, max_sys_results)
  
  # Read start datetime and reporting time step
  header$start_datetime <- read_num(con, 1, size = 8)
  header$report_step = read_int(con)
  
  # Save current file pointer offset
  offsets$output_start <- seek(con, where = NA)
  
  # Read datasets from the end of the file 
  n_bytes <- 4 * length(field_config$footer)
  seek(con, - n_bytes, origin = "end")
  footer <- read_functions$footer(con, 1)
  
  # Calculate block sizes
  block_sizes = data.frame(
    subcatch = object_counts$subcatch * (length(subcatch_vars) + n_polluts),
    nodes = object_counts$node * (length(node_vars) + n_polluts),
    links = object_counts$link * (length(link_vars) + n_polluts),
    system = max_sys_results
  )
  
  list(
    header = header,
    subcatch = subcatch,
    nodes = nodes,
    links = links,
    subcatch_vars = subcatch_vars,
    node_vars = node_vars,
    link_vars = link_vars,
    sys_result_ids = sys_result_ids,
    offsets = as.data.frame(offsets),
    block_sizes = block_sizes,
    footer = footer
  )
}

# get_metadata_field_config ----------------------------------------------------
get_metadata_field_config <- function()
{
  list(
    
    header = list(
      magic_number = "int",
      version = "int",
      flow_units = "int"
    ),
    
    footer = list(
      offset_ids = "int",
      offset_inputs = "int",
      offset_ouputs = "int",
      n_periods = "int",
      error_code = "int",
      magic_number = "int"
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
      volume = "int",
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
}

# get_read_function ------------------------------------------------------------
get_read_function <- function(...)
{
  read_functions <- list(int = read_int, num = read_num)
  
  function(con, i) {
    as.data.frame(stringsAsFactors = FALSE, lapply(list(...), function(x) {
      read_functions[[x]](con)
    }))
  }
}

# read_names -------------------------------------------------------------------
read_names <- function(con, n)
{
  sapply(seq_len(n), function(i) readChar(con, readBin(con, "integer")))
}

# read_int ---------------------------------------------------------------------
read_int <- function(con, n = 1)
{
  readBin(con, "integer", n)
}

# read_objects -----------------------------------------------------------------
read_objects <- function(con, n, ids, fun)
{
  stopifnot(identical(read_int(con), n))
  
  read_int(con, n)
  
  data <- do.call(rbind, lapply(ids, fun, con = con))
  
  data.frame(name = ids, data, stringsAsFactors = FALSE)
}

# read_object_vars -------------------------------------------------------------
read_object_vars <- function(con, n, read_function, n_polluts)
{
  stopifnot(identical(read_int(con), n))
  
  result <- read_function(con, 1)
  
  read_int(con, n_polluts)
  
  result
}

# read_num ---------------------------------------------------------------------
read_num <- function(con, n = 1, size = 4)
{
  readBin(con, "numeric", n, size = size)
}
