// 
// sql_LV++ DSO/DLL library 
// Author: Danny Holstein
// Desc:   Interface between LabVIEW and database APIs (MySQL, ODBC, etc; Connectors)
//         Mostly through LV data-to-flattened string, so that the data can be passed back and 
//         forth efficiently and without printf/sscanf() formating (lossless)
//  
//         The C++ class maintains the connection, environment, and includes database API 
//         error information for workbench-type/enhanced DB debugging.  Also, we check for
//         NULL references and keep track of all the objects, which means we won't suffer 
//         SEGFAULTS if we wire in the wrong, or closed reference. Canaries are used on both 
//         ends of the object to make sure nothing has been clobbered.
// 
//  NOTE:  At some point, I plan to write a library to convert LabVIEW Variant to C++ std::variant *,
//         the resulting DB library would then be usable by all extensible (through DLLs), interpreting 
//         languages.  Of course, the calling language would also have to delete these when through.
//

#define SQL_LVPP_VERSION "sql_LVPP-2.0"
#ifndef ODBCAPI
//#define MYAPI       //  MySQL C Connector
//#define MYCPPAPI    //  MySQL Connector/C++
#define ODBCAPI     //  ODBC
#endif


#ifdef WIN
    #include <windows.h>
    #include "extcode.h" //  LabVIEW external code
    #include <string>
    typedef unsigned int uint;
    typedef unsigned char u_char;
    typedef unsigned short u_int16_t;
    typedef unsigned long u_int32_t;
    #ifndef ODBCAPI
    #define ODBCAPI     //  always available in Windows
    #endif
#else
    #include <arpa/inet.h>
    #include "/usr/local/lv71/cintools/extcode.h" //  LabVIEW external code
    #include <string.h>
    typedef int errno_t;
#endif

#include <stdlib.h>
#include <stdio.h>
#include <iostream> // std::cout
#include <list>     //  container of generated objects for error checking (avoid SEGFAULT)
#include <variant>  //  container for field data
#define VAR_TYPES char, short, long, unsigned long, float, char*, void*, double, string
#include <vector>   //  container for results
#include <array>   //  container for results
#include <algorithm>    // std::min
#include <memory>

using namespace std;

#if 1 //  LabView declarations

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
typedef ResultSet** ResultSetHdl;
typedef struct {
    long dimSizes[2];
    LStrHandle elt[1];
} DataSet;
typedef DataSet** DataSetHdl;
typedef struct {
    long dimSize; //  TD array dimension
    unsigned char TypeDescriptor[1]; //  Array of LabVIEW types corresponding to expected result set
} Types;
typedef Types** TypesHdl;

//  LabVIEW string utilities
#define LStrString(A) string((char*) (*A)->str, (*A)->cnt)
void LV_str_cp(LStrHandle LV_string, string c_str)
{
    DSSetHandleSize(LV_string, sizeof(int) + c_str.length() * sizeof(char));
    (*LV_string)->cnt = c_str.length();
    memcpy((char*)(*LV_string)->str, &(c_str[0]), c_str.length());
}
void LV_str_cat(LStrHandle LV_string, string str)    //  concatenate C++ str to LSTR LV_string
{
    int n = (*LV_string)->cnt;
    DSSetHandleSize(LV_string, sizeof(int) + n + str.length());
    (*LV_string)->cnt = n + str.length();
    memcpy((char*)(*LV_string)->str + n, str.c_str(), str.length());
}
void LV_str_cat(LStrHandle LV_string, string str, int size)    //  concatenate C++ str to LSTR LV_string
{
    int n = (*LV_string)->cnt;
    DSSetHandleSize(LV_string, sizeof(int) + ((*LV_string)->cnt = n + size));
    memcpy((char*)(*LV_string)->str + n, str.c_str(), size);
}
void LV_str_cat(LStrHandle LV_string, char* str, int size)    //  concatenate C-style str to LSTR LV_string
{
    int n = (*LV_string)->cnt;
    DSSetHandleSize(LV_string, sizeof(int) + ((*LV_string)->cnt = n + size));
    memcpy((char*)(*LV_string)->str + n, str, size);
}
void LV_strncpy(LStrHandle LV_string, char* c_str, int size)
{
    DSSetHandleSize(LV_string, sizeof(int) + size * sizeof(char));
    (*LV_string)->cnt = size;
    memcpy((char*)(*LV_string)->str, c_str, size);
}
void LV_strncpy(LStrHandle LV_string, string str, int size)
{
    DSSetHandleSize(LV_string, sizeof(int) + size + 1);
    (*LV_string)->cnt = size;
    memcpy((char*)(*LV_string)->str, str.c_str(), size); ((*LV_string)->str)[size] = 0;
}
LStrHandle LVStr(string str)    //  convert string to new LV string handle
{
    LStrHandle l; if ((l = (LStrHandle) DSNewHClr(sizeof(int32) + str.length())) == NULL) return NULL;
    memcpy((char*)(*l)->str, str.c_str(), ((*l)->cnt = str.length()));
    return l;
}
LStrHandle LVStr(string str, int size)
{
    LStrHandle l; if ((l = (LStrHandle) DSNewHClr(sizeof(int32) + size)) == NULL) return NULL;
    memcpy((char*)(*l)->str, str.c_str(), ((*l)->cnt = size));
    return l;
}
LStrHandle LVStr(char* str, int size)
{
    LStrHandle l; if ((l = (LStrHandle) DSNewHClr(sizeof(int32) + size)) == NULL) return NULL;
    memcpy((char*)(*l)->str, str, ((*l)->cnt = size));
    return l;
}

#endif

#ifdef MYAPI
#include "/usr/include/mysql/mysql.h"
#endif
#ifdef MYCPPAPI
#include "mysql_connection.h"
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
#endif
#ifdef ODBCAPI
#include <sql.h>
#include <sqlext.h>
#define ODBC_ERROR(t, o, d) {\
                SQLCHAR buf[1024]; SQLSMALLINT TextLength;\
                SQLGetDiagRec(t, o, 1, 0, 0, buf, 1024, &TextLength);\
                errstr = string((char*)buf, TextLength); errnum = -1; errdata = d;\
                }
#endif

#define MAGIC 0x13131313    //  doesn't necessarily need to be unique to this, specific library
                            //  the odds another library class will be exactly the same length are low

class LvDbLib {       // LabVIEW MySQL Database class
public:
    uint canary_begin = MAGIC; //  check for buffer overrun/corruption
    int errnum;       // error number
    string errstr;    // error description
    string errdata;   // data which precipitated error
    string SQLstate;  // SQL state
    uint16_t type;    // RDMS type, see enum db_type.h
    int StrBufLen = 256;    // initialize to 256
    int StrBlobLen = 4096;  // Used when StrBufLen==0 as buffer length for BLOBs

    union API
    {
#ifdef MYAPI
        struct tMY {
            MYSQL* con;           //  connection
            MYSQL_RES* query_results; //  result set
            MYSQL_STMT* stmt;          //  prepared statement
            MYSQL_BIND* bind;          //  API places data for the bound columns into these specified buffers
        } my;  //  MySQL Connector
#endif
#ifdef MYCPPAPI
        struct tMYCPP {
            sql::Driver* driver;  //  driver
            sql::Connection* con; //  connection
            sql::Statement* stmt; //  SQL query as object
            sql::ResultSet* res;  //  result set
        } mycpp;  //  MySQL Connector/C++
#endif
#ifdef ODBCAPI
        struct tODBC {
            SQLHENV     hEnv;
            SQLHDBC     hDbc;
            SQLHSTMT    hStmt;
        } odbc;  //  ODBC
#endif
    } api;

#include "db_type.h"
#include "LvTypeDescriptors.h"

    LvDbLib(string ConnectionString, string user, string pw, string db, u_int16_t t) { //  contructor and open connection
        switch (t)
        {
        case NULL:
            break;

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            break;
#endif

#ifdef MYAPI
        case MySQL:
            break;
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            break;
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS, type = " + to_string(t);
            break;
        }

        type = t; errnum = 0; errstr = "SUCCESS";
        if (ConnectionString.length() < 1) {api.odbc.hDbc = NULL; errnum = -1; errstr = "Connection string may not be blank"; }
        else {
            switch (type)
            {
            case NULL:
                break;

#ifdef ODBCAPI
            case ODBC:
            case SqlServer:
               {SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &(api.odbc.hEnv));
                SQLSetEnvAttr(api.odbc.hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
                SQLAllocHandle(SQL_HANDLE_DBC, api.odbc.hEnv, &(api.odbc.hDbc));
                string cs = ConnectionString;
                if (user != "") cs += "UID=" + user + ";";
                if (pw != "") cs += "PWD=" + pw + ";";
                SQLRETURN rc = SQLDriverConnect(api.odbc.hDbc, NULL, (SQLCHAR*)cs.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
                if (rc == SQL_ERROR) ODBC_ERROR(SQL_HANDLE_DBC, api.odbc.hDbc, cs);}
                break;
#endif

#ifdef MYAPI
            case MySQL:
                if ((api.my.con = mysql_init(NULL)) == NULL)
                    {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); break;}
                if (mysql_real_connect(api.my.con, ConnectionString.c_str(),
                    user.c_str(), pw.c_str(), db.c_str(), 0, "/run/mysql/mysql.sock", 0) == NULL)
                    {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);}
                StrBufLen = 256; break;
#endif

#ifdef MYCPPAPI
            case MySQLpp:
                try
                {
                    api.mycpp.driver = get_driver_instance();     //  Create a driver instance
                    api.mycpp.con = (api.mycpp.driver)->connect(ConnectionString, user, pw);  //  Connect to the MySQL Connector/C++
                }
                catch (sql::SQLException& e)
                {
                    errstr = e.what();          errdata = ConnectionString;
                    errnum = e.getErrorCode();  SQLstate = e.getSQLState();
                }
                break;
#endif

            default:
                errnum = -1; errstr = "Unsupported RDBMS, type = " + to_string(t);
                break;
            }
        }
    }

    ~LvDbLib() {  //  close connections and free handles
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            mysql_close(api.my.con);
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            if (api.odbc.hDbc == NULL) break;
            SQLDisconnect(api.odbc.hDbc);
            SQLFreeHandle(SQL_HANDLE_DBC, api.odbc.hDbc);
            SQLFreeHandle(SQL_HANDLE_ENV, api.odbc.hEnv);
            break;
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            api.mycpp.con->close();
            delete api.mycpp.con; // delete api.mycpp.driver; <- something about it being virtual
            api.mycpp.con = NULL;
            break;
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS";
            break;
        }
    }

    int SetSchema(string schema) {  //  set DB schema
        errnum = 0; errdata = schema;
        if (schema.length() < 1) { errstr = "Schema string may not be blank"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            return Execute("USE " + schema);
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            (void) Execute("USE " + schema);
            return errnum;
            break;
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            if (api.mycpp.con == NULL) { errstr = "Connection closed"; errnum = -1; return -1; }
            try { api.mycpp.con->setSchema(schema); }
            catch (sql::SQLException& e) {
                errstr = e.what();
                errnum = e.getErrorCode();  SQLstate = e.getSQLState();
            }
            return !errnum ? 0 : -1;
            break;
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS";
            break;
        }
        return errnum;
    }

    int Query(string query, int cols) {  //  run query against connection and put results in res
        errnum = -1; errdata = query;
        if (query.length() < 1) { errstr = "Query string may not be blank"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
            if (!(api.my.stmt = mysql_stmt_init(api.my.con)))
                {errnum = -1; errstr = "Out of memory"; return -1;}
            if (mysql_stmt_prepare(api.my.stmt, query.c_str(), query.length())) //  Prepare statement
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
            api.my.bind = new MYSQL_BIND[cols];
            if (mysql_stmt_execute(api.my.stmt))                    //  Execute
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
            if (mysql_stmt_bind_param(api.my.stmt, api.my.bind))    //  bind results
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);
                 mysql_stmt_close(api.my.stmt); return -1;}
            errnum = 0; errdata = ""; return 0;
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            {
            int rc;
            rc = SQLAllocHandle(SQL_HANDLE_STMT, api.odbc.hDbc, &(api.odbc.hStmt));
            if (rc == SQL_ERROR) {ODBC_ERROR(SQL_HANDLE_DBC, api.odbc.hDbc, query); return -1;}
            rc = SQLExecDirect(api.odbc.hStmt, (SQLCHAR*)query.c_str(), SQL_NTS);
            if (rc == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);
                 SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
            return 0;
            }
            break;
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            if (api.mycpp.con == NULL) { errstr = "Connection closed"; return -1; }
            try {
                api.mycpp.stmt = api.mycpp.con->createStatement();
                api.mycpp.res = api.mycpp.stmt->executeQuery(query);
                errnum = 0; errstr = "SUCCESS"; return api.mycpp.res->rowsCount();
            }
            catch (sql::SQLException& e) {
                errstr = e.what();          errdata = query;
                errnum = e.getErrorCode();  SQLstate = e.getSQLState(); return -1;
            }
            break;
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS";
            break;
        }
        return errnum;
    }

    int Execute(string query) {  //  run query against connection and return num rows affected
        errnum = 0; errdata = query; int ans = 0;
        if (query.length() < 1) { errstr = "Query string may not be blank"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
            if (mysql_real_query(api.my.con, query.c_str(), query.length()))
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); ans = -1;}
            else
                {errnum = 0; errdata = ""; ans = mysql_affected_rows(api.my.con);}
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            if (!api.odbc.hDbc) { errnum = -1; errstr = "No DB connection"; return -1; }
            if (SQLAllocHandle(SQL_HANDLE_STMT, api.odbc.hDbc, &(api.odbc.hStmt)) == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "SQLAllocHandle"); return -1;}
            int rc; rc = SQLExecDirect(api.odbc.hStmt, (SQLCHAR*)query.c_str(), SQL_NTS);
            if (rc == SQL_ERROR) {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query); ans = -1;}
            else SQLRowCount(api.odbc.hStmt, (SQLLEN*)&ans);
            SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt);
            break;
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            if (api.mycpp.con == NULL) { errstr = "Connection closed"; return -1; }
            try {
                api.mycpp.stmt = api.mycpp.con->createStatement(); ans = api.mycpp.stmt->executeUpdate(query);
                errnum = 0; errdata = ""; errstr = "SUCCESS"; delete api.mycpp.stmt;
            }
            catch (sql::SQLException& e) {
                errstr = e.what();          errdata = query;
                errnum = e.getErrorCode();  SQLstate = e.getSQLState(); ans = -1;
            }
            break;
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS";
            break;
        }
        return ans;
    }

    int UpdatePrepared(string query, string v[], int rows, int cols, uint16_t ColsTD[]) {  //  UPDATE/INSERT etc with flattened LabVIEW data
        errnum = -1; errdata = query; int i, j, ans = -1;
        if (query.length() < 1) { errstr = "Query string may not be blank"; return -1; }
        if (rows * cols == 0) { errstr = "No data to post"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
#define CASE(xTD, cType, sType) case  xTD:\
    union { cType f; char c[sizeof(cType)]; } xTD ## _;\
    memcpy(xTD ## _.c, val.c_str(), sizeof(cType));\
    bind[i].buffer_type = sType; bind[i].buffer = (char*)&xTD ## _.f;\
    bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length


        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
            api.my.stmt = mysql_stmt_init(api.my.con);
            if (api.my.stmt == NULL) { errstr = "Out of memory"; return -1; }
            if (mysql_stmt_prepare(api.my.stmt, query.c_str(), query.length()))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);
                mysql_stmt_close(api.my.stmt); return -1;
            }
            MYSQL_BIND* bind; bind = new MYSQL_BIND[cols];

            for (j = 0; j < rows; j++)
            {
                for (i = 0; i < cols; i++)
                {
                    string val = v[j * cols + i];
                    switch (ColsTD[i])
                    {
                    case U8:
                    case Boolean:
                    CASE(I8, char, MYSQL_TYPE_TINY)
                        break;
                    case U16:
                    CASE(I16, short, MYSQL_TYPE_SHORT)
                        break;
                    case U32:
                    CASE(I32, int, MYSQL_TYPE_LONG)
                        break;
                    CASE(SGL, float, MYSQL_TYPE_FLOAT)
                        break;
                    CASE(DBL, double, MYSQL_TYPE_DOUBLE)
                        break;
                    case String:
                    case Array: //  how we pass BLOB data (not null-terminated str)
                        char* str_data; str_data = (char*) val.c_str();
                        long unsigned str_length; str_length = val.length();
                        bind[i].buffer_type = (ColsTD[i] != Array? MYSQL_TYPE_STRING: MYSQL_TYPE_BLOB);
                        bind[i].buffer = (char*)str_data;
                        bind[i].buffer_length = str_length; bind[i].is_null = 0; bind[i].length = &str_length;
                        break;
                    default:
                        {errstr = "Data type (" + to_string(ColsTD[i]) + ") not supported"; return -1; }
                        break;
                    }
                }
                if ((errnum = mysql_stmt_bind_param(api.my.stmt, bind)) != 0)
                    {errstr = mysql_error(api.my.con); mysql_stmt_close(api.my.stmt); return -1;}
                if (mysql_stmt_execute(api.my.stmt) != 0)
                    {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);
                     mysql_stmt_close(api.my.stmt); return -1;}
            }
            ans = j; mysql_stmt_close(api.my.stmt); delete bind;
            {errnum = 0; errstr = "SUCCESS"; return ans; }
            break;
#undef CASE
#endif

#ifdef ODBCAPI
#define CASE(LVt, SQLt, SQLCt, Ct) \
    case LVt:\
        union { Ct f; char c[sizeof(Ct)]; } LVt ## _;\
        memcpy(LVt ## _.c, (*val).c_str(), sizeof(Ct));\
        rc = SQLBindParameter(api.odbc.hStmt, i + 1, SQL_PARAM_INPUT, \
            SQLt, SQLCt, sizeof(Ct), 0, &LVt ## _.f, 0, (SQLLEN*) &cbValue);\
        if (rc == SQL_ERROR) \
            {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);\
             SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}

        case ODBC:
        case SqlServer:
            if (api.odbc.hDbc == NULL) { errnum = -1; errstr = "Connection closed"; return -1; }
            if (SQLAllocHandle(SQL_HANDLE_STMT, api.odbc.hDbc, &(api.odbc.hStmt)) == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "SQLAllocHandle"); return -1;}
            int rc; rc = SQLPrepare(api.odbc.hStmt, (SQLCHAR*)query.c_str(), SQL_NTS);
            if (rc == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);
                 SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
            for (j = 0; j < rows; j++)
            {
                for (i = 0; i < cols; i++)
                {
                    SQLINTEGER cbValue; 
                    string *val = &v[j * cols + i];
                    switch (ColsTD[i]) {    //  NOTE: We may want to use SQL_C_DEFAULT instead of specific C type
                        CASE(Boolean, SQL_C_BIT, SQL_BIT, u_char)
                            break;
                        CASE(U8, SQL_C_UTINYINT, SQL_TINYINT, u_char)
                            break;
                        CASE(I8, SQL_C_STINYINT, SQL_TINYINT, char)
                            break;
                        CASE(U16, SQL_C_USHORT, SQL_SMALLINT, int16)
                            break;
                        CASE(I16, SQL_C_SSHORT, SQL_SMALLINT, uInt16)
                            break;
                        CASE(U32, SQL_C_ULONG, SQL_INTEGER, int32)
                            break;
                        CASE(I32, SQL_C_SLONG, SQL_INTEGER, uint32_t)
                            break;
                        CASE(SGL, SQL_C_FLOAT, SQL_REAL, float)
                            break;
                        CASE(DBL, SQL_C_DOUBLE, SQL_DOUBLE, double)
                            break;
                        case String:
                            cbValue = SQL_NTS;
                            rc = SQLBindParameter(api.odbc.hStmt, i + 1, SQL_PARAM_INPUT,SQL_C_CHAR, 
                                SQL_LONGVARCHAR, (*val).length(), 0, (SQLCHAR*) &((*val)[0]), (*val).length(), (SQLLEN*) &cbValue);
                            if (rc == SQL_ERROR)
                                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);
                                    SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
                            break;
                        case Array: //  how we pass binary data (not SQL_NTS/null-terminated str)
                            cbValue = (*val).length();
                            rc = SQLBindParameter(api.odbc.hStmt, i + 1, SQL_PARAM_INPUT,SQL_C_BINARY, 
                                SQL_VARBINARY, (*val).length(), 0, (SQLCHAR*) &((*val)[0]), (*val).length(), (SQLLEN*) &cbValue);
                            if (rc == SQL_ERROR)
                                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);
                                    SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
                            break;
                        default:
                            {errstr = "Data type (" + to_string(ColsTD[i]) + ") not supported"; return -1; }
                            break;
                    }
                }
                rc = SQLExecute(api.odbc.hStmt);
                if (rc == SQL_ERROR)
                    {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);
                     SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
            }
            SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); ans = j;
            break;
#undef CASE
#endif

#ifdef MYCPPAPI
        case MySQLpp:
            if (api.mycpp.con == NULL) { errnum = -1; errstr = "Connection closed"; return -1; }
            try {
                sql::PreparedStatement* pstmt; pstmt = api.mycpp.con->prepareStatement(query);
                for (j = 0; j < rows; j++)
                {
                    for (i = 0; i < cols; i++)
                    {
                        string val = v[j * cols + i];
                        switch (ColsTD[i])
                        {
                        case I8:
                            union { char f; char c[sizeof(char)]; } I8_;
                            I8_.c[0] = val[0]; pstmt->setInt(i + 1, I8_.f);
                            break;
                        case U8:
                        case Boolean:
                            union { u_char f; char c[sizeof(char)]; } U8_;
                            U8_.c[0] = val[0]; pstmt->setUInt(i + 1, U8_.f);
                            break;
                        case I32:
                            union { int f; char c[sizeof(int)]; } I32_;
                            memcpy(I32_.c, val.c_str(), sizeof(int)); pstmt->setInt(i + 1, I32_.f);
                            break;
                        case U32:
                            union { uint f; char c[sizeof(uint)]; } U32_;
                            memcpy(U32_.c, val.c_str(), sizeof(int)); pstmt->setInt(i + 1, U32_.f);
                            break;
                        case DBL:
                            union { double f; char c[sizeof(double)]; } DBL_;
                            memcpy(DBL_.c, val.c_str(), sizeof(double)); pstmt->setDouble(i + 1, DBL_.f);
                            break;
                        case String:
                            pstmt->setString(i + 1, val);
                            break;
                        case Array: //  how we pass BLOB data (not null-terminated str)
                            std::stringstream *stream; stream->write(& val[0], val.length());
                            pstmt->setBlob(i + 1, stream);
                            break;
                        default:
                            break;
                        }
                    }
                    pstmt->executeUpdate();
                }
                delete pstmt; return j;
            }
            catch (sql::SQLException& e) {
                errstr = e.what();          errdata = query;
                errnum = e.getErrorCode();  SQLstate = e.getSQLState(); return -1;
            }          break;
#endif

        default:
            errstr = "Unsupported RDBMS"; return -1;
            break;
        }
        return ans;
    }

    int GetResults(int *rows, int cols, TypesHdl types, ResultSetHdl results) {  //  return results as LV flattened strings
        errnum = 0; errdata = ""; int rc;
        int row = 0; //  row number
        vector<SQLLEN> DataLen(cols, 0);
        vector<string> str(cols, string(StrBufLen, (char) 0));
        vector<variant<VAR_TYPES>> res(cols); // result set. MSVC has heartburn with initialization "(cols, (char) 0)"

        if (*rows > 0)  //  we know the number of rows before hand, otherwise we need to dynamically allocate on fetches
           {DSSetHandleSize(results, sizeof(int32) * 2 + *rows * cols * sizeof(LStrHandle));
            (**results).dimSizes[0] = *rows; (**results).dimSizes[1] = cols;}

        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL: {
            if (!(api.my.query_results = mysql_stmt_result_metadata(api.my.stmt))) //  Fetch result set meta information
                { errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return false;}
            if (cols != mysql_num_fields(api.my.query_results))
                {errnum = -1; errstr = "Data column number mismatch"; return false;}

            /* Fetch result set meta information */
            MYSQL_FIELD* fields; fields = mysql_fetch_fields(api.my.query_results);

            vector<unsigned long> length(cols);
            vector<my_bool*> error(cols), is_null(cols);

            for (int i = 0; i < cols; i++) {  //  bind buffers
                api.my.bind[i].is_null = is_null[i]; api.my.bind[i].error = error[i];
                api.my.bind[i].length = &length[i]; api.my.bind[i].buffer_type = fields[i].type;
                switch (fields[i].type)
                {
                case MYSQL_TYPE_TINY:
                    res[i] = (char) 0; api.my.bind[i].buffer = &(res[i]);
                    break;
                case MYSQL_TYPE_SHORT:
                    res[i] = (short) 0; api.my.bind[i].buffer = &(res[i]);
                    break;
                case MYSQL_TYPE_LONG:
                    res[i] = (long) 0; api.my.bind[i].buffer = &(res[i]);
                    break;
                case MYSQL_TYPE_FLOAT:
                    res[i] = (float) 0; api.my.bind[i].buffer = &(res[i]);
                    break;
                case MYSQL_TYPE_DOUBLE:
                    res[i] = (double) 0; api.my.bind[i].buffer = &(res[i]);
                    break;
                case MYSQL_TYPE_TINY_BLOB ... MYSQL_TYPE_STRING:
                    if (StrBufLen)
                       {api.my.bind[i].buffer = (char*) str[i].c_str();
                        api.my.bind[i].buffer_length = StrBufLen;}
                    else
                       {api.my.bind[i].buffer = 0;
                        api.my.bind[i].buffer_length = 0;}
                    break;
                default:
                    delete api.my.bind;
                    errnum = -1; errstr = "Unsupported MySQL type: " + to_string(fields[i].type);
                    mysql_free_result(api.my.query_results); mysql_stmt_close(api.my.stmt);
                    return -1;
                    break;
                }
            }
            if (mysql_stmt_bind_result(api.my.stmt, api.my.bind))
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
#define CASE(xTD, cType) case  xTD:\
            (**results).elt[row * cols + i] = LVStr((char*) &(res[i]), length[i]);

            while ((rc = mysql_stmt_fetch(api.my.stmt)) != 1) {  //  Fetch all rows
                if (rc == MYSQL_NO_DATA) break;
                //  allocate another row
                int err = mFullErr;
                err = DSSetHandleSize(results, sizeof(int32) * 2 + (row+1) * cols * sizeof(LStrHandle));
                (**results).dimSizes[0] = (row+1); (**results).dimSizes[1] = cols;

                for (int i = 0; i < cols; i++)
                {
                    // NOTE:  NULL DB results map only to LStr NULL string -> LStr NULL variant
                    //        The only LV TD that has a something we can use for NULL is float/double (NaN)
                    //        In the re-conversion to Numeric/String, NULL Variants should be an error state

                    if (!is_null[i])
                    {
                        switch ((**types).TypeDescriptor[i])
                        {
                        case I8:
                        case U8:
                        CASE(Boolean, char)
                            break;
                        case U16:
                        CASE(I16, short)
                            break;
                        case U32:
                        CASE(I32, long)
                            break;
                        CASE(SGL, float)
                            break;
                        CASE(DBL, double)
                            break;
                        case  String:
                        case  Array:
                        default:
                            if (length[i] > StrBufLen)
                            {
                                int retval; if ((retval = mysql_stmt_fetch_column(api.my.stmt, api.my.bind, i, 0)) != 0)
                                    {errnum = retval; errstr = mysql_error(api.my.con); return -1;}
                            }
                            else
                                (**results).elt[row * cols + i] = LVStr((char*) str[i].c_str(), length[i]);
                            break;
                        }
                    }
                }
                row++; 
            }
            if (rc == 1)
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}

            delete api.my.bind;
            mysql_free_result(api.my.query_results);
            if (mysql_stmt_close(api.my.stmt))
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
            errnum = 0; errdata = ""; errstr = "SUCCESS";
            break;}
#undef CASE
#endif

#ifdef ODBCAPI
#if 1   //  CASE() MACRO
#define CASE(xTD, cType, sType)   case  xTD:\
    res[i] = (cType) 0; DataLen[i] = sizeof(cType);\
    if (StrBufLen) rc = SQLBindCol(api.odbc.hStmt, i + 1, sType, &(res[i]),\
        sizeof(cType), &(DataLen[i]));\
    else rc = 0;\
    if (rc == SQL_ERROR)\
    {\
        ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "Column " + to_string(i + 1) + "; type " + to_string(t));\
        SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;\
    }
#endif
        case ODBC:
        case SqlServer:
            for (SQLUSMALLINT i = 0; i < cols; i++)
            {
                int t = (**types).TypeDescriptor[i];
                switch (t)
                {  //  any of these numeric type might overflow, that's what we'd like to catch
                CASE(Boolean, char, SQL_C_BIT)
                    break;
                case  I8:
                CASE(U8, char, SQL_C_CHAR)
                    break;
                CASE(U32, unsigned long, SQL_C_ULONG)
                    break;
                CASE(I32, long, SQL_C_LONG)
                    break;
                CASE(SGL, float, SQL_C_FLOAT)
                    break;
                CASE(DBL, double, SQL_C_DOUBLE)
                    break;
                case String:
                case Array: //  Looking at documentation, it appears SQL_C_BINARY and SQL_C_CHAR are interchangeable
                    if (StrBufLen) //  StrBufLen == 0 means don't bind, and use SQLGetData() after SQLFetch()
                    {
                        DataLen[i] = StrBufLen;
                        rc = SQLBindCol(api.odbc.hStmt, i + 1, (t == String ? SQL_C_CHAR: SQL_C_BINARY),
                                    (char*) str[i].c_str(), DataLen[i], &(DataLen[i]));
                        if (rc == SQL_ERROR)
                        {
                            ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "Column " + to_string(i + 1) + "; type " + to_string(t));
                            SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return false;
                        }
                    }
                    else 
                       {DataLen[i] = 0;
                        rc = SQLBindCol(api.odbc.hStmt, i + 1, (t == String ? SQL_C_CHAR: SQL_C_BINARY),
                                    0, DataLen[i], &(DataLen[i]));
                        if (rc == SQL_ERROR)
                        {
                            ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "Column " + to_string(i + 1) + "; type " + to_string(t));
                            SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return false;
                        }}
                    break;
                default:
                    errnum = -1; errstr = "Unsupported data type: " + to_string(t);
                    return errnum;
                    break;
                }

            }
#undef CASE

#if 1   //  CASE() MACRO
#define CASE(xTD, cType, sType) \
     case  xTD:\
            if (StrBufLen == 0){\
                DataLen[i] = sizeof(cType);\
                rc = SQLGetData(api.odbc.hStmt, i + 1, sType, &(res[i]), DataLen[i], & DataLen[i]);\
                if (rc == SQL_ERROR)\
                {\
                    ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "Column " + to_string(i + 1) + "; type " + to_string(t));\
                    SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;\
                }}\
            (**results).elt[row * cols + i] = LVStr((char*) &(res[i]), sizeof(cType));
#endif

            row = 0; rc = SQLFetch(api.odbc.hStmt);
            if (rc == SQL_ERROR)
            {
                ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "");
                SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return false;
            }
            while (rc  != SQL_NO_DATA)
            {
                if (rc == SQL_ERROR)
                {
                    ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "");
                    SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;
                }
                //  allocate another row
                DSSetHandleSize(results, sizeof(int32) * 2 + (row + 1) * cols * sizeof(LStrHandle));
                (**results).dimSizes[0] = (row + 1); (**results).dimSizes[1] = cols;
                for (SQLUSMALLINT i = 0; i < cols; i++)
                {
                    int t = (**types).TypeDescriptor[i];
                    switch (t)
                    {
                    CASE(Boolean, char, SQL_C_BIT)
                        break;
                    case  I8:
                    CASE(U8, char, SQL_C_CHAR)
                        break;
                    case I32:
                    CASE(U32, u_int32_t, SQL_C_ULONG)
                        break;
                    CASE(SGL, float, SQL_C_FLOAT)
                        break;
                    CASE(DBL, double, SQL_C_DOUBLE)
                        break;
                    case String:
                    case Array:
                        if (StrBufLen)
                           {(**results).elt[row * cols + i] = LVStr((char*) str[i].c_str(), DataLen[i]);
                            if (DataLen[i] > StrBufLen)
                                {errnum = -1; errstr = "Truncated data, column:" + to_string(i + 1); return -1;}}
                        else
                        {
                            bool Init = true; SQLINTEGER RtnDataLen = StrBlobLen;
                            DataLen[i] = RtnDataLen; string str = string(RtnDataLen, (char) 0);
                            while ((rc = SQLGetData(api.odbc.hStmt, i + 1, (t == Array ? SQL_C_BINARY: SQL_C_CHAR), (char*) str.c_str(),
                                DataLen[i], (SQLLEN*) &RtnDataLen)) != SQL_NO_DATA)  //  DataLen[i] will be equal to SqlBlobLen until the last call
                            {  
                                if (rc == SQL_ERROR) break;
                                if (DataLen[i] == SQL_NULL_DATA) break; //  it appears field == NULL, leave results string NULL
                                int NumBytes = (RtnDataLen > DataLen[i]) || (RtnDataLen == SQL_NO_TOTAL) ?
                                    DataLen[i] - (t == Array ? 0 : 2) : RtnDataLen; //  "2" may correspond to wchar termination
                                if (Init) {(**results).elt[row * cols + i] = LVStr(str, NumBytes); Init = false;}
                                else LV_str_cat((**results).elt[row * cols + i], str, NumBytes);
                            }
                            if (rc == SQL_ERROR)
                            {
                                ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "");
                                SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt);
                                return errnum;
                            }
                        }
                        break;
                    default:
                        break;
                    }
                }
                rc = SQLFetch(api.odbc.hStmt); row++;
            }
#undef CASE            
            break;
#endif

#ifdef MYCPPAPI
#define CASE(xTD, cType, method) \
     case  xTD:\
            union { cType d; char str[sizeof(cType)]; } xTD ## _;\
            xTD ## _.d = res->method(i + 1);\
            (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(cType));\
            LV_strncpy((**results).elt[row * cols + i], xTD ## _.str, sizeof(cType));
                            
        case MySQLpp:
            try {
                sql::ResultSet* res = api.mycpp.res; //  sql::ResultSet
                while (res->next())
                {
                    for (int i = 0; i < cols; i++) {
                        if (!res->isNull(i + 1))
                            switch ((**types).TypeDescriptor[i])
                            {
                            case  Boolean:  //  any of these numeric type might overflow, that's what we'd like to catch
                            CASE(U8, u_char, getInt)
                                break;
                            CASE(I8, char, getInt)
                                break;
                            CASE(U16, u_int16_t, getInt)
                                break;
                            CASE(I16, int16_t, getInt)
                                break;
                            CASE(U32, u_int32_t, getInt)
                                break;
                            CASE(I32, int32, getInt)
                                break;
                            CASE(SGL, float, getDouble)
                                break;
                            CASE(DBL, double, getDouble)
                                break;
                            case  String:
                            default:
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(0);
                                LV_str_cp((**results).elt[row * cols + i], res->getString(i + 1));  //  <- std::string, handles binaries
                                break;
                            }
                    }
                    row++;
                }
                errnum = 0; errdata = ""; errstr = "SUCCESS"; ans = true;
            }
            catch (sql::SQLException& e) {
                errstr = e.what();;
                errnum = e.getErrorCode();  SQLstate = e.getSQLState(); ans = false;
            }

            delete api.mycpp.res; delete api.mycpp.stmt;
            break;
#undef CASE
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS"; return -1;
            break;
        }
        // for (int i = 0; i < cols; i++) if (str[i] != NULL) delete str[i];
        return (*rows = row);
    }

    uint canary_end = MAGIC;  //  check for buffer overrun/corruption
};

class ObjList
{
public:
    LvDbLib* addr;
    bool deleted;
    ObjList(LvDbLib* a) { addr = a; deleted = false; }

    bool operator< (const ObjList rhs) const { return addr < rhs.addr; }
    bool operator<= (const ObjList rhs) const { return addr <= rhs.addr; }
    bool operator== (const ObjList rhs) const { return addr == rhs.addr; }
};
static std::list<ObjList> myObjs;

static string ObjectErrStr; //  where we store user-checked/non-API error messages
static bool   ObjectErr;    //  set to "true" for user-checked/non-API error messages

bool IsObj(LvDbLib* addr) //  check for corruption/validity, use <list> to track all open connections, avoid SEGFAULT
{
    if (addr == NULL) { ObjectErrStr = "NULL DB object"; ObjectErr = true; return false; }
    bool b = false;
    for (auto i : myObjs) { if (i == ObjList(addr)) b = true; }
    if (!b) { ObjectErrStr = "Invalid DB object (unallocated memory or non-DB reference)"; ObjectErr = true; return false; }

    if (addr->canary_begin == MAGIC && addr->canary_end == MAGIC) { ObjectErr = false; ObjectErrStr = "SUCCESS"; return true; }
    else { ObjectErr = true; ObjectErrStr = "Object memory corrupted"; return false; }
}

extern "C" {  //  functions to be called from LabVIEW.  'extern "C"' is necessary to prevent overload name mangling

    LvDbLib* OpenDB(LStrHandle ConnectionString, LStrHandle user,
        LStrHandle pw, LStrHandle db, u_int16_t type) { //  open DB connection
        LvDbLib* LvDbObj = new LvDbLib(LStrString(ConnectionString), LStrString(user),
            LStrString(pw), LStrString(db), type);
        ObjList* o = new ObjList(LvDbObj); myObjs.push_back(*o);  //  keep record of all objects to check against SEGFAULT
        return LvDbObj; //  return pointer to LvDbLib object
    }

    int SetSchema(LvDbLib* LvDbObj, LStrHandle schema) { //  set DB schema
        if (!IsObj(LvDbObj)) return -1;
        LvDbObj->SetSchema(LStrString(schema));
        return LvDbObj->errnum;
    }

    int Execute(LvDbLib* LvDbObj, LStrHandle query) { //  run query against connection and return num rows affected
        if (!IsObj(LvDbObj)) return -1;
        return LvDbObj->Execute(LStrString(query));
    }

    int UpdatePrepared(LvDbLib* LvDbObj, LStrHandle query, DataSetHdl data, uint16_t ColsTD[]) { //  run prepared statement and return num rows affected
        if (!IsObj(LvDbObj)) return -1;
        int rows = (**data).dimSizes[0]; int cols = (**data).dimSizes[1];
        string* vals = new string[rows * cols]; //  replace with unique_ptr
        for (int j = 0; j < rows; j++)
            for (int i = 0; i < cols; i++) {
                LStrHandle s = (**data).elt[j * cols + i];
                if (ColsTD[i] == LvDbObj->String)
                    vals[j * cols + i] = LStrString(s) + '\0';
                else
                    vals[j * cols + i] = LStrString(s);
            }
        int NumRows = LvDbObj->UpdatePrepared(LStrString(query), vals, rows, cols, ColsTD);
        delete[] vals; return NumRows;
    }

    int Query(LvDbLib* LvDbObj, LStrHandle query, TypesHdl types, ResultSetHdl results) { //  run query against connection and return result set in flattened strings
        int rows, cols = (**types).dimSize; if (cols == 0) return 0;  //  number of columns, return if no data columns requested  
        if (!IsObj(LvDbObj)) return -1;
        if ((rows = LvDbObj->Query(LStrString(query), cols)) < 0) return -1; //  std::string version of SQL query
        if (LvDbObj->GetResults(&rows, cols, types, results) < 0) return -1;
        else return rows;
    }

    int CloseDB(LvDbLib* LvDbObj) { //  close DB connection and free memory
        if (!IsObj(LvDbObj)) return -1;
        myObjs.remove(LvDbObj); delete LvDbObj; return 0;
    }

#if 1    //  the following are utility-ish functions
    void GetError(LvDbLib* LvDbObj, tLvDbErr* error) { //  get error info from LvDbLib object properties
        if (LvDbObj == NULL || ObjectErr) {
            if (!ObjectErr) return; //  no error
            error->errnum = -1;
            LV_str_cp(error->errstr, ObjectErrStr);
            ObjectErr = false; ObjectErrStr = "NO ERROR"; //  Clear error, but race conditions may exist, if so, da shit has hit da fan. 
        }
        else {
            error->errnum = LvDbObj->errnum;
            LV_str_cp(error->errstr, LvDbObj->errstr);
            LV_str_cp(error->errdata, LvDbObj->errdata);
            LV_str_cp(error->SQLstate, LvDbObj->SQLstate);
            LvDbObj->errnum = 0; LvDbObj->errstr = ""; LvDbObj->errdata = ""; LvDbObj->SQLstate = ""; //  clear error info
        }
    }

    int Type(LvDbLib* LvDbObj) { //  get DB API type
        if (!IsObj(LvDbObj)) return -1;
        return LvDbObj->type;
    }

    int SetBufLen(LvDbLib* LvDbObj, int len, char tBuf) { //  set string buffer length, set to zero (0) for blobs and other large strings for dynamic allocation
        if (!IsObj(LvDbObj)) return -1;
        switch (tBuf)
        {
        case 0:
        default:
            LvDbObj->StrBufLen = len;
            break;
        case 1:
            LvDbObj->StrBlobLen = len;
            break;
        }
        return 0;
    }

    int GetBufLen(LvDbLib* LvDbObj, char tBuf) { //  get string buffer length (to restore after retrieval of BLOB, etcetera)
        if (!IsObj(LvDbObj)) return -1;
        switch (tBuf)
        {
        case 0:
        default:
            return LvDbObj->StrBufLen;
        case 1:
            return LvDbObj->StrBlobLen;
        }
    }

    char *Version() {return (char*) string(SQL_LVPP_VERSION).c_str();};
#endif
}