
/* Standard C++ includes */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>     // std::cout
#include <list>

#include "mysql_connection.h"
#include <arpa/inet.h>
#include "/usr/local/lv71/cintools/extcode.h" //  LabVIEW external code

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#define MAGIC 0x13131313

using namespace std;

class LvDbLib {       // LabVIEW MySQL Database class
  public:
    uint canary_begin = MAGIC; //  check for buffer overrun and that we didn't delete object
    int errnum;       // error number
    string errstr;    // error description
    string errdata;   // data precipitating error
    string SQLstate;  // SQL state

    sql::Driver *driver;  //  driver
    sql::Connection *con; //  opened connection
    sql::Statement *stmt; //  SQL query as object
    sql::ResultSet *res;  //  result set

    #include "/home/danny/LV/SqlLV++/LvTypeDescriptors.h"

    LvDbLib(string ConnectionString, string user, string pw) { //  contructor and open connection
      errnum = 0; errstr  = "SUCCESS";
      if (ConnectionString.length() < 1) {errnum = -1; errstr = "Connection string may not be blank";}
      else {
        try {
          driver = get_driver_instance();                     //  Create a connection
          con = driver->connect(ConnectionString, user, pw);  //  Connect to the MySQL database
        }
        catch (sql::SQLException &e) {
          errstr = e.what();          errdata = ConnectionString;
          errnum = e.getErrorCode();  SQLstate = e.getSQLState();
        }
      }
    }

    int SetSchema(string schema) {  //  set DB schema
      errnum = 0; errdata = schema;
      if (schema.length() < 1) {errstr = "Schema string may not be blank"; return -1;}
      if (con == NULL) {errstr = "Connection closed"; return -1;}
      try {con->setSchema(schema);}
      catch (sql::SQLException &e) {
        errstr = e.what();
        errnum = e.getErrorCode();  SQLstate = e.getSQLState();
      }
      return !errnum ? 0 : -1;
    }

    int Query(string query) {  //  run query against connection and put results in res
      errnum = -1; errdata = query;
      if (query.length() < 1) {errstr = "Query string may not be blank"; return -1;}
      if (con == NULL) {errstr = "Connection closed"; return -1;}
      try {
        stmt = con->createStatement(); res = stmt->executeQuery(query);
        errnum = 0; errdata = ""; errstr = "SUCCESS"; return res->rowsCount();}
      catch (sql::SQLException &e) {
        errstr = e.what();          errdata = query;
        errnum = e.getErrorCode();  SQLstate = e.getSQLState(); return -1;
      }
    }

    int Execute(string query) {  //  run query against connection and return num rows affected
      errnum = -1; errdata = query; int ans = 0;
      if (query.length() < 1) {errstr = "Query string may not be blank"; return -1;}
      if (con == NULL) {errstr = "Connection closed"; return -1;}
      try {
        stmt = con->createStatement(); ans = stmt->executeUpdate(query);
        errnum = 0; errdata = ""; errstr = "SUCCESS"; delete stmt;}
      catch (sql::SQLException &e) {
        errstr = e.what();          errdata = query;
        errnum = e.getErrorCode();  SQLstate = e.getSQLState(); ans = -1;
      }
      return ans;
    }

    int UpdatePrepared(string query, string v[], int rows, int cols, uint16_t ColsTD[]) {  //  UPDATE/INSERT etc with flattened LabVIEW data
      errnum = -1; errdata = query; int i, j;
      if (query.length() < 1) {errstr = "Query string may not be blank"; return -1;}
      if (rows * cols == 0) {errstr = "No data to post"; return -1;}
      if (con == NULL) {errstr = "Connection closed"; return -1;}
      try {
        sql::PreparedStatement *pstmt; pstmt = con->prepareStatement(query); 
        for (j = 0; j < rows; j++)
        {
          for (i=0; i<cols; i++)
          {
            string val = v[j*cols + i]; 
            switch (ColsTD[i])
            {
            case I8:
              union {char f; char c[sizeof(char)];} I8_;
              I8_.c[0] = val[0]; pstmt->setInt(i+1, I8_.f);
              break;
            case U8:
            case Boolean:
              union {u_char f; char c[sizeof(char)];} U8_;
              U8_.c[0] = val[0]; pstmt->setUInt (i+1, U8_.f);
              break;
            case I32:
              union {int f; char c[sizeof(int)];} I32_;
              memcpy(I32_.c, val.c_str(), sizeof(int)); pstmt->setInt(i+1, I32_.f);
              break;
            case U32:
              union {uint f; char c[sizeof(uint)];} U32_;
              memcpy(U32_.c, val.c_str(), sizeof(int)); pstmt->setInt(i+1, U32_.f);
              break;
            case DBL:
              union {double f; char c[sizeof(double)];} DBL_;
              memcpy(DBL_.c, val.c_str(), sizeof(double)); pstmt->setDouble(i+1, DBL_.f);
              break;
            case String:
              pstmt->setString(i+1, val);
              break;
            default:
              break;
            }
          }
        pstmt->executeUpdate();
        }
        delete pstmt; return j;
      }
      catch (sql::SQLException &e) {
        errstr = e.what();          errdata = query;
        errnum = e.getErrorCode();  SQLstate = e.getSQLState(); return -1;
      }
    }

    uint canary_end = MAGIC;  //  check for buffer overrun and that we didn't delete object
};

class ObjList
{
  public:
    LvDbLib *addr;
    bool deleted;
    ObjList(LvDbLib *a) {addr = a; deleted = false;}

    inline bool operator< (ObjList rhs) { return addr < rhs.addr; }
    inline bool operator== (ObjList rhs) { return addr == rhs.addr; }
    inline bool operator<= (ObjList rhs) { return addr <= rhs.addr; }
};
static std::list<ObjList> myObjs;

#ifndef LIB
#if 0
int main(void)
{
  cout << endl;
  cout << "Running 'SELECT 'Hello World!' AS _message'..." << endl;

  try {
    sql::Driver *driver;
    sql::Connection *con;
    sql::Statement *stmt;
    sql::ResultSet *res;

    /* Create a connection */
    driver = get_driver_instance();
    con = driver->connect("tcp://127.0.0.1:3306", "danny", "danny13");
    /* Connect to the MySQL test database */
    con->setSchema("test");

    stmt = con->createStatement();
    res = stmt->executeQuery("SELECT 'Hello World!' AS _message");
    while (res->next()) {
      cout << "\t... MySQL replies: ";
      /* Access column data by alias or column name */
      cout << res->getString("_message") << endl;
      cout << "\t... MySQL says it again: ";
      /* Access column data by numeric offset, 1 is the first column */
      cout << res->getString(1) << endl;
    }
    delete res;
    delete stmt;
    delete con;
  }
  catch (sql::SQLException &e) {
    cout << "# ERR: SQLException in " << __FILE__;
    cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
    cout << "# ERR: " << e.what();
    cout << " (MySQL error code: " << e.getErrorCode();
    cout << ", SQLState: " << e.getSQLState() << " )" << endl;
  }

  cout << endl;

  return EXIT_SUCCESS;
} 
#else
int main(void) {
  LvDbLib L("tcp://127.0.0.1:3306", "danny", "danny13"); L.SetSchema("test");
  LvDbLib *ptrL = &L;

  cout << "Open connection: " << ptrL->errstr << endl; //  cout << L.errstr << endl;
  cout << "rowsCount: " << ptrL->Query("SELECT 'Hello World!' AS _message, 3.1 as FLT") << endl;
  cout << "Query: " << ptrL->errstr << endl;
  
  // cout << "rowsCount: " << ptrL->res->rowsCount() << endl;

  cout << "column cnt: " << ptrL->res->getMetaData()->getColumnCount() << endl;

}
#endif

#endif

typedef struct {
	long errnum;
	LStrHandle errstr;
	LStrHandle errdata;
	LStrHandle SQLstate;
	} tLvDbErr;
typedef struct {
  long dimSizes[2];
  LStrHandle elt[1];
  } ResultSet;
typedef ResultSet **ResultSetHdl;
typedef struct {
  long dimSizes[2];
  LStrHandle elt[1];
  } DataSet;
typedef DataSet **DataSetHdl;
typedef struct {
	long dimSize; //  TD array dimension
	unsigned short TypeDescriptor[1]; //  Array of LabVIEW types corresponding to expected result set
	} Types;
typedef Types **TypesHdl;
static string ObjectErrStr; //  where we store user-checked/non-API error messages
static bool ObjectErr;      //  set to "true" for user-checked/non-API error messages

bool IsObj(LvDbLib* addr) //  check for corruption/validity, add <list> to track all open connections, avoid SEGFAULT
{  
  if (addr == NULL) {ObjectErrStr = "NULL DB object"; ObjectErr = true; return false;}
  bool b = false; 
  for (auto i : myObjs) {if (i == ObjList(addr)) b = true;}
  if (!b) {ObjectErrStr = "Invalid DB object (unallocated memory or non-DB reference)"; ObjectErr = true; return false;}

  if (addr->canary_begin == MAGIC && addr->canary_end == MAGIC) {ObjectErr = false; ObjectErrStr = "SUCCESS"; return true;}
  else {ObjectErr = true; ObjectErrStr = "Object memory corrupted"; return false;}
 }

//  LabVIEW string utilities
void LV_str_cp(LStrHandle LV_string, char* c_str)
{
  DSSetHandleSize(LV_string, sizeof(int) + (strlen(c_str) + 1)*sizeof(char));
  (*LV_string)->cnt=strlen(c_str);
  strcpy((char*) (*LV_string)->str, c_str);
}
void LV_str_cp(LStrHandle LV_string, string c_str)
{
  DSSetHandleSize(LV_string, sizeof(int) + (c_str.length() + 1)*sizeof(char));
  (*LV_string)->cnt = c_str.length();
  strcpy((char*) (*LV_string)->str, c_str.c_str());
}
void LV_str_cat(LStrHandle LV_string, char* c_str)
{
  int n = (*LV_string)->cnt;
  DSSetHandleSize(LV_string, n + sizeof(int) + (strlen(c_str) + 1)*sizeof(char));
  (*LV_string)->cnt = n + strlen(c_str);
  strcpy((char*) (*LV_string)->str+n, c_str);
}
void LV_strncpy(LStrHandle LV_string, char* c_str, int size)
{
  DSSetHandleSize(LV_string, sizeof(int) + size*sizeof(char));
  (*LV_string)->cnt=size;
  strncpy((char*) (*LV_string)->str, c_str, size);
}

extern "C" {  //  functions to be called from LabVIEW.  'extern "C"' is necessary to prevent overloading name mangling

LvDbLib* OpenDB(char* ConnectionString, char* user, char* pw) { //  open DB connection
  std::string cs(ConnectionString); std::string u(user); std::string pass(pw);
  LvDbLib *LvDbObj = new LvDbLib(cs, u, pass);
  ObjList *o = new ObjList(LvDbObj); myObjs.push_back(*o);  //  keep record of all objects to check against SEGFAULT
  return LvDbObj; //  return pointer to LvDbLib object
}

int SetSchema(LvDbLib* addr, char* schema) { //  set DB schema
  if (!IsObj(addr)) return -1; LvDbLib *LvDbObj = addr;
  std::string schem(schema); LvDbObj->SetSchema(schem);
  return LvDbObj->errnum;
}

int Execute(LvDbLib* addr, char* query) { //  run query against connection and return num rows affected
  if (!IsObj(addr)) return -1; LvDbLib *LvDbObj = addr;
  std::string q(query); return LvDbObj->Execute(q);
}

int UpdatePrepared(LvDbLib* addr, char* query, DataSetHdl data, uint16_t ColsTD[]) { //  run prepared statement and return num rows affected
  if (!IsObj(addr)) return -1; LvDbLib *LvDbObj = addr;
  int rows = (**data).dimSizes[0]; int cols = (**data).dimSizes[1];
  string * vals = new string[rows * cols]; 
  for (int j = 0; j < rows; j++)
    for (int i = 0; i < cols; i++) {
      LStrHandle s = (**data).elt[j*cols + i];
      vals[j*cols + i] = (std::string((char*) (*s)->str, (*s)->cnt)) ;}
  std::string q(query); return LvDbObj->UpdatePrepared(q, vals, rows, cols, ColsTD);
}

int Query(LvDbLib* addr, char* query, TypesHdl types, ResultSetHdl results) { //  run query against connection and return result set in flattened strings
  int cols = (**types).dimSize; if (cols == 0) return 0;  //  number of columns, return if no data columns requested  
  int row = 0; //  row number
  if (!IsObj(addr)) return -1; LvDbLib *LvDbObj = addr;
  std::string q(query); int rows = LvDbObj->Query(q); //  std::string version of SQL query
  if (rows<0) return -1;
  sql::ResultSet *res = LvDbObj->res;                 //  sql::ResultSet
  DSSetHandleSize(results, sizeof(int32)*2 + rows*cols*sizeof(LStrHandle));
  (**results).dimSizes[0] = rows; (**results).dimSizes[1] = cols;

  try {
    while (res->next())
    {
      for (int i=0; i<cols; i++) {
        switch ((**types).TypeDescriptor[i])
        {
          case  LvDbObj->Boolean:  //  any of these numeric type might overflow, that's what we'd like to catch
          case  LvDbObj->U8:
            union {u_char f; char c[sizeof(char)];} U8;
            U8.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(char));
            LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
            break;
          case  LvDbObj->I8:
            union {char f; char c[sizeof(char)];} I8;
            I8.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(char));
            LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
            break;
          case  LvDbObj->U16:
            union {u_int16_t f; char c[sizeof(u_int16_t)];} U16;
            U16.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(u_int16_t));
            LV_strncpy((**results).elt[row * cols + i], U16.c, sizeof(u_int16_t));
            break;
          case  LvDbObj->I16:
            union {u_int16_t f; char c[sizeof(u_int16_t)];} I16;
            I16.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(int16_t));
            LV_strncpy((**results).elt[row * cols + i], I16.c, sizeof(int16_t));
          case  LvDbObj->U32:
            union {u_int32_t f; char c[sizeof(u_int32_t)];} U32;
            U32.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(u_int32_t));
            LV_strncpy((**results).elt[row * cols + i], U32.c, sizeof(u_int32_t));
            break;
          case  LvDbObj->I32:
            union {u_int32_t f; char c[sizeof(u_int32_t)];} I32;
            I32.f = res->getInt(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(int32_t));
            LV_strncpy((**results).elt[row * cols + i], I32.c, sizeof(int32_t));
            break;
          case  LvDbObj->SGL:
            union {float f; char c[sizeof(float)];} SGL;
            SGL.f = res->getDouble(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(float));
            LV_strncpy((**results).elt[row * cols + i], SGL.c, sizeof(float));
            break;
          case  LvDbObj->DBL:
            union {double f; char c[sizeof(double)];} DBL;
            DBL.f = res->getDouble(i+1);
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(double));
            LV_strncpy((**results).elt[row * cols + i], DBL.c, sizeof(double));
            break;
          case  LvDbObj->String:
          default:
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(0);
            LV_str_cp((**results).elt[row * cols + i], res->getString(i+1));
            break;
        }
      }
      row++;
    }
    LvDbObj->errnum = 0; LvDbObj->errdata = ""; LvDbObj->errstr = "SUCCESS";}
  catch (sql::SQLException &e) {
    LvDbObj->errstr = e.what(); LvDbObj->errdata = query;
    LvDbObj->errnum = e.getErrorCode();  LvDbObj->SQLstate = e.getSQLState(); return -1;
  }

  delete res; delete LvDbObj->stmt;
  return rows;
}

int CloseDB(LvDbLib* addr) { //  close DB connection and free memory
  if (addr == NULL) {ObjectErrStr = "NULL DB object"; ObjectErr = true; return -1;}
  if (!IsObj(addr)) return -1;
  myObjs.remove(addr); delete addr; return 0;
}

void GetError(LvDbLib* addr, tLvDbErr *error) { //  get error info from LvDbLib object properties
  if (addr == NULL || ObjectErr) {
    if (!ObjectErr) return; //  no error
    error->errnum = -1;
    LV_str_cp(error->errstr,   ObjectErrStr);
    ObjectErr = false; ObjectErrStr = "NO ERROR"; //  Clear error, but race conditions may exist, if so, da shit has hit da fan. 
  }
  else {
    LvDbLib LvDbObj = *addr;
    error->errnum = LvDbObj.errnum;
    LV_str_cp(error->errstr,   LvDbObj.errstr);
    LV_str_cp(error->errdata,  LvDbObj.errdata);
    LV_str_cp(error->SQLstate, LvDbObj.SQLstate);
    LvDbObj.errnum = 0; LvDbObj.errstr = ""; LvDbObj.errdata = ""; LvDbObj.SQLstate = ""; //  clear error info
  }
}

}
