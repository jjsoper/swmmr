#include <Rcpp.h>
using namespace Rcpp;

#define DEBUG 0
#define ERROR_FILE_OPEN 1
#define ERROR_FILE_SEEK 2
#define ERROR_FILE_TELL 3
#define MAX_VAR_VALUES 15
#define N_INPUT_RECORDS(x, n) (1 + n * (x + 1))

// dummy variable to store array of chars
char buffer[80]; 

int    SWMM_version;                   // SWMM version
int    SWMM_Nperiods;                  // number of reporting periods
int    SWMM_FlowUnits;                 // flow units code
int    SWMM_Npolluts;                  // number of pollutants tracked
double SWMM_StartDate;                 // start date of simulation
int    SWMM_ReportStep;                // reporting time step (seconds)

//int    RunSwmmExe(char* cmdLine);
//int    RunSwmmDll(char* inpFile, char* rptFile, char* outFile);
//int    OpenSwmmOutFile(char* outFile);
//int    GetSwmmResult(int iType, int iIndex, int vIndex, int period);
//int    CloseSwmmOutFile();

static const int SUBCATCH = 0;
static const int NODE     = 1;
static const int LINK     = 2;
static const int SYS      = 3;
static const int RECORDSIZE = 4;       // number of bytes per file record

static int n_objects[4]; // SUBCATCH, NODE, LINK, SYS
static int n_records[4]; // SUBCATCH, NODE, LINK, SYS

static int n_records_skip[4]; // SUBCATCH, NODE, LINK, SYS

// number of reporting variables
static int n_variables[4]; // SUBCATCH, NODE, LINK, SYS

static FILE*  Fout;                    // file handle
static int    OutputStartPos;          // file position where results start
static double BytesPerPeriod;          // bytes used for results in each period

// Variable values of all objects of one type (SUBCATCH, NODE, LINK, SYS) 
// at one point in time
//static float var_values[MAX_VAR_VALUES];
static float* var_values = NULL;
static int max_n_variables = 0;
static int max_n_objects = 0;

//-----------------------------------------------------------------------------
int open_output_file(const char* outFile)
{
  if ((Fout = fopen(outFile, "rb")) == NULL) {
    
    printf("Could not open '%s' for binary reading.\n", outFile);
    
  } else {
    
    printf("File %s opened.\n", outFile);
  }

  return (Fout != NULL);
}

//-----------------------------------------------------------------------------
int file_seek(off_t offset, int whence)
{
  /*if (debug) printf(
    "fseeko(Fout, offset = %jd, whence = %d) ... \n",
    (intmax_t) offset, whence
  );*/
  
  if (fseeko(Fout, offset, whence) != 0) {
    
    return 0;
  }
  
  return 1;
}

//-----------------------------------------------------------------------------
int read_bytes(void* pointer, const char* name, int n_bytes, int n_records)
{
  //printf("Reading %s ... ", name);
  
  size_t n_read = fread(pointer, n_bytes, n_records, Fout);

  if (n_read != n_records) {
    
    if (n_records == 1) {
      printf("Reading %s failed.\n", name);
    } 
    else {
      printf("Reading %d %s failed.\n", n_records, name);
    }
    
    return 0;
  }
  
  //printf("ok.\n");

  return 1;
}

//-----------------------------------------------------------------------------
int read_record(void* pointer, const char* name)
{
  return read_bytes(pointer, name, RECORDSIZE, 1);
}

//-----------------------------------------------------------------------------
int read_record_double(void* pointer, const char* name)
{
  return read_bytes(pointer, name, sizeof(double), 1);
}  

//-----------------------------------------------------------------------------
int file_tell(off_t least_expected)
{
  // use ftello() instead of ftell():
  // https://stackoverflow.com/questions/16696297/ftell-at-a-position-past-2gb
  off_t result = ftello(Fout);
  
  if (result < least_expected) {
    
    printf("File is not as big as expected.\n");
    printf("- ftello returned: %jd\n", (intmax_t) result);
    printf("- least_expected: %jd\n", (intmax_t) least_expected);
    
    return 0;
  }
  
  return 1;
}

//-----------------------------------------------------------------------------
int read_names(std::vector<std::string> &names, const char* type_name)
{
  int n_chars;
  
  for (int i = 0; i < names.size(); i++) {

    if (! read_record(&n_chars, type_name)) {
      return 0;
    }

    names[i] = fgets(buffer, n_chars + 1, Fout);
  }
  
  if (DEBUG) {
    
    printf("%d %ss read.\n", names.size(), type_name);
  }
  
  return 1;
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
List OpenSwmmOutFile(const char* outFile)
{
  int magic1, magic2, errCode, InputStartPos;
  off_t offset;

  // --- open the output file
  if (! open_output_file(outFile)) {
    return List::create(_["error"] = ERROR_FILE_OPEN);
  }
  
  // --- check that file contains at least 14 records
  if (! file_seek(0, SEEK_END)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  if (! file_tell(14 * RECORDSIZE)) {
    return List::create(_["error"] = ERROR_FILE_TELL);
  }

  // --- read parameters from end of file
  if (! file_seek(-5 * RECORDSIZE, SEEK_END)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }

  read_record(&InputStartPos, "InputStartPos");
  read_record(&OutputStartPos, "OutputStartPos");
  read_record(&SWMM_Nperiods, "SWMM_Nperiods");
  read_record(&errCode, "errCode");
  read_record(&magic2, "magic2");

  if (DEBUG) {
    
    printf("InputStartPos: %d\n", InputStartPos);   // in SWMM: InputStartPos
    printf("OutputStartPos: %d\n", OutputStartPos); // in SWMM: OutputStartPos
    printf("SWMM_Nperiods: %d\n", SWMM_Nperiods);
    printf("errCode: %d\n", errCode);
    printf("magic2: %d\n", magic2);
  }
  
  // --- read magic number from beginning of file
  if (! file_seek(0, SEEK_SET)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  read_record(&magic1, "magic1");

  // --- quit if errors found
  if (int error = (magic1 != magic2 || errCode != 0 || SWMM_Nperiods == 0)) {
    return List::create(_["error"] = error);
  }

  // --- otherwise read additional parameters from start of file
  read_record(&SWMM_version, "SWMM_version");
  read_record(&SWMM_FlowUnits, "SWMM_FlowUnits");
  
  read_record(&(n_objects[SUBCATCH]), "n_objects[SUBCATCH]");
  read_record(&(n_objects[NODE]), "n_objects[NODE]");
  read_record(&(n_objects[LINK]), "n_objects[LINK]");
  
  read_record(&SWMM_Npolluts, "SWMM_Npolluts");

  // --- define name vectors
  std::vector<std::string> Namesubcatch(n_objects[SUBCATCH]);
  std::vector<std::string> Namenodes(n_objects[NODE]);
  std::vector<std::string> Namelinks(n_objects[LINK]);
  std::vector<std::string> Namepolls(SWMM_Npolluts);
  
  // --- read object names from file
  read_names(Namesubcatch, "subcatchment");
  read_names(Namenodes, "node");
  read_names(Namelinks, "link");
  read_names(Namepolls, "pollutant");
  
  // Skip over saved subcatch/node/link input values
  offset = (off_t) InputStartPos + (off_t) RECORDSIZE * (
    N_INPUT_RECORDS(n_objects[SUBCATCH], 1) + // Subcatchment area
    N_INPUT_RECORDS(n_objects[NODE], 3) + // Node type, invert & max depth
    N_INPUT_RECORDS(n_objects[LINK], 5) // Link type, z1, z2, max depth & length
  );
  
  if (! file_seek(offset, SEEK_SET)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  // Read number & codes of computed variables
  //read_record(&SubcatchVars, "SubcatchVars"); // # Subcatch variables
  read_record(&(n_variables[SUBCATCH]), "n_variables[SUBCATCH]"); // # Subcatch variables

  if (! file_seek(n_variables[SUBCATCH] * RECORDSIZE, SEEK_CUR)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  read_record(&(n_variables[NODE]), "n_variables[NODE]"); // # Node variables
  
  if (! file_seek(n_variables[NODE] * RECORDSIZE, SEEK_CUR)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  read_record(&(n_variables[LINK]), "n_variables[LINK]"); // # Link variables
  
  if (! file_seek(n_variables[LINK] * RECORDSIZE, SEEK_CUR)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }
  
  read_record(&(n_variables[SYS]), "n_variables[SYS]"); // # System variables
  
  // --- read data just before start of output results
  if (! file_seek(OutputStartPos - 3 * RECORDSIZE, SEEK_SET)) {
    return List::create(_["error"] = ERROR_FILE_SEEK);
  }

  read_record_double(&SWMM_StartDate, "SWMM_StartDate");
  read_record(&SWMM_ReportStep, "SWMM_ReportStep");

  if (DEBUG) {
    
    printf("n_variables[SUBCATCH]: %d\n", n_variables[SUBCATCH]);
    printf("n_variables[NODE]: %d\n", n_variables[NODE]);
    printf("n_variables[LINK]: %d\n", n_variables[LINK]);
    printf("n_variables[SYS]: %d\n", n_variables[SYS]);
    
    printf("SWMM_StartDate: %f\n", SWMM_StartDate);
    printf("SWMM_ReportStep: %d\n", SWMM_ReportStep);
  }

  // calculate helper variables that will be used to determine the position
  // of result data in the output file 
  int n_record_sum = 0;

  // Set the number of system "objects" to 1, just to be consistent and to be 
  // able to let the following loop go from 0 (SUBCATCH) to 3 (SYS)
  n_objects[SYS] = 1;
  max_n_variables = 0;
  max_n_objects = 0;
  
  for (int i = SUBCATCH; i <= SYS; i++) {
    
    n_records[i] = n_objects[i] * n_variables[i];
    n_record_sum += n_records[i];
    
    if (i > SUBCATCH) {
      n_records_skip[i] = n_records_skip[i - 1] + n_records[i - 1];      
    } 
    else {
      n_records_skip[i] = 0;
    }
    
    // update maximum number of variables per category
    if (n_variables[i] > max_n_variables) {
      max_n_variables = n_variables[i];
    }
    
    // update maximum number of objects per category
    if (n_objects[i] > max_n_objects) {
      max_n_objects = n_objects[i];
    }
  }
  
  // --- compute number of bytes of results values used per time period
  // date value (a double)
  BytesPerPeriod = RECORDSIZE * (2 + n_record_sum);

  // Allocate memory for all variables of all objects of one type
  var_values = (float*) calloc(max_n_objects * max_n_variables, sizeof(float));

  // --- return with file left open
  return List::create(
    _["meta"] = List::create(_["version"] = SWMM_version),
    _["subcatchments"] = List::create(_["names"] = Namesubcatch),
    _["nodes"] = List::create(_["names"] = Namenodes),
    _["links"] = List::create(_["names"] = Namelinks),
    _["pollutants"] = List::create(_["names"] = Namepolls)
  );
}

//-----------------------------------------------------------------------------
int restrict_to_range(int i, int from, int to, const char* name) {
  
  if (i < from) {
    printf("Setting %s (%d) to min allowed value: %d\n", name, i, from);
    i = from;
  } 
  else if (i > to) {
    printf("Setting %s (%d) to max allowed value: %d\n", name, i, to);
    i = to;
  }
  
  return i;
}

//-----------------------------------------------------------------------------
int warn_if_not_equal_and_set_to_zero(int iIndex, int expected)
{
  if (iIndex != expected) {
    
    printf(
      "iIndex is not %d as expected but: %d. Anyway using iIndex = 0.",
      expected, iIndex
    );
  }
  
  return 0;
}  

// ----------------------------------------------------------------------------
void print_integer_vector(Rcpp::IntegerVector v, const char* name)
{
  printf("%s [0:%d]:", name, v.size() - 1);
  for (int i = 0; i < v.size(); i++) {
    printf(" %d", v[i]);
  }
  printf("\n");
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
Rcpp::NumericVector GetSwmmResultPart_(
  int iType, 
  Rcpp::IntegerVector objIndices, 
  Rcpp::IntegerVector varIndices, 
  int firstPeriod, 
  int lastPeriod
)
{
  int n_to_read = -1;
  int success = 0;
  int n_vars = varIndices.size();
  int n_objs = objIndices.size();
  int n = SWMM_Nperiods;
  
  firstPeriod = restrict_to_range(firstPeriod, 1, n, "firstPeriod");
  lastPeriod = restrict_to_range(lastPeriod, firstPeriod, n, "lastPeriod");

  // Count number of variables, number of objects (e.g. nodes, links) 
  // and records
  int n_periods = lastPeriod - firstPeriod + 1;
  
  // Do we read only a single value each time?
  int single_value = (n_objs == 1 && n_vars == 1);

  // Initialise result vector with a special number that indicates errors
  std::vector<float> result(n_vars * n_objs * n_periods, -9999.9999);
  int result_index = 0;

  if (iType != SUBCATCH && iType != NODE && iType != LINK && iType != SYS) {
    
    printf("Unknown iType: %d\n", iType);
    return wrap(result);
  }

  if (iType == SYS) {
    
    objIndices[0] = warn_if_not_equal_and_set_to_zero(objIndices[0], 777);
  }

  // --- compute offset into output file
  off_t offset = (off_t) OutputStartPos + RECORDSIZE * (
    2 + 
      n_records_skip[iType] + 
      objIndices[0] * n_variables[iType] + 
      (single_value ? varIndices[0] : 0)
  );

  // Now that the object indices have been used to determine the offset, we
  // modify them so that the first index is 0
  for (int i = 0; i < n_objs; i++) {
    
    objIndices[i] = objIndices[i] - objIndices[0];
  }

  if (DEBUG) {
    
    print_integer_vector(objIndices, "objIndices after reindexing");
    printf("single_value: %s\n", single_value ? "yes" : "no");
  }
  
  // determine how many floating point numbers to read
  if (! single_value) {
    
    //n_to_read = (objIndices[n_objs] - objIndices[0]) * n_variables[iType] +
    n_to_read = objIndices[n_objs - 1] * n_variables[iType] +
      varIndices[n_vars - 1] + 1;

    if (DEBUG) {
      
      printf(
        "n_to_read = objIndices[%d] * n_variables[%d] + varIndices[%d] + 1\n",
        n_objs-1, iType, n_vars-1
      );
      printf(
        "          = %d * %d + %d + 1 = %d\n", 
        objIndices[n_objs-1], n_variables[iType], varIndices[n_vars-1], n_to_read
      );
    }
  }
  
  for (int i = firstPeriod; i <= lastPeriod; i++) {

    // --- re-position the file pointer and read the result
    if (! file_seek(offset + (off_t) (i - 1) * BytesPerPeriod, SEEK_SET)) {
      
      return wrap(result);
    }

    if (single_value) {
      
      // Read directly into result vector
      success = read_bytes(&result[i - firstPeriod], "values", RECORDSIZE, 1);
    }
    else {
      
      // Read a block of values into var_values first
      success = read_bytes(var_values, "values", RECORDSIZE, n_to_read);
    }

    if (! success) {
      
      printf("Error reading result values from the output file.\n");
      return wrap(result);
    }
    
    // If more than one value was read, copy the values into the result vector
    if (! single_value) {
      
      for (int obj = 0; obj < n_objs; obj++) {
        
        for (int var = 0; var < n_vars; var++) {
          
          int from = objIndices[obj] * n_variables[iType] + varIndices[var];

          if (from >= max_n_objects * max_n_variables) {
            printf("Invalid from index: %d!\n", from);
            return wrap(result);
          } else {
            //printf("obj/var/from: %d/%d/%d\n", obj, var, from);
          }
          
          //printf("result_index: %d/%d\n", result_index, n_objects * n_vars * n_periods - 1);
          
          if (result_index < n_objs * n_vars * n_periods) {
            result[result_index++] = var_values[from];
          }
          else {
            printf("Invalid result_index: %d!\n", result_index);
            return wrap(result);
          }
        }
      }
    }
  }

  return wrap(result);
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
Rcpp::NumericVector GetSwmmResultPart(
  int iType, int objIndex, int varIndex, int firstPeriod, int lastPeriod
)
{
  Rcpp::IntegerVector objIndices(1, objIndex);
  Rcpp::IntegerVector varIndices(1, varIndex);
  
  //print_integer_vector(objIndices, "objIndices");
  //print_integer_vector(varIndices, "varIndices");
  
  return GetSwmmResultPart_(
    iType, objIndices, varIndices, firstPeriod, lastPeriod
  );
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
Rcpp::NumericVector GetSwmmResultPart2(
  int iType, int objIndex, Rcpp::IntegerVector varIndices, 
  int firstPeriod, int lastPeriod
)
{
  Rcpp::IntegerVector objIndices(1, objIndex);

  //print_integer_vector(objIndices, "objIndices");
  //print_integer_vector(varIndices, "varIndices");
  
  return GetSwmmResultPart_(
    iType, objIndices, varIndices, firstPeriod, lastPeriod
  );
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
Rcpp::NumericVector GetSwmmResult(int iType, int iIndex, int vIndex)
{
  return GetSwmmResultPart(iType, iIndex, vIndex, 1, SWMM_Nperiods);
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
Rcpp::NumericVector GetSwmmTimes()
{
  // Initialise vector with all elements equal to start date in seconds 
  Rcpp::NumericVector timesvec(SWMM_Nperiods, SWMM_StartDate * 86400);

  // Add a multiple of the report time step to each element
  for (int i = 0; i < SWMM_Nperiods; i++) {
    
    timesvec[i] += (double) SWMM_ReportStep * (i + 1);
  }
  
  return timesvec;
}

//-----------------------------------------------------------------------------
// [[Rcpp::export]]
int CloseSwmmOutFile()
{
  // Free memory for all variables of all objects of one type
  if (var_values != NULL) {
    free(var_values);
    var_values = NULL;
  }
  
  if (Fout != NULL) {
    fclose(Fout);
    Fout = NULL;
    return 1;
  }
  
  return 0;
}
