#include "pch.h"
// #define MYAPI   //  MySQL C Connector
// #define MYCPPAPI  //  MySQL Connector/C++
#define ODBCAPI //  ODBC

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>     // std::cout
#include <list>

#ifndef WIN
#include <arpa/inet.h>
#else
#include <windows.h>
typedef unsigned int uint;
typedef unsigned char u_char;
typedef unsigned short u_int16_t;
#include <string>
#endif

using namespace std;

#if 1 //  LabView declarations
#include "extcode.h" //  LabVIEW external code

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
    unsigned short TypeDescriptor[1]; //  Array of LabVIEW types corresponding to expected result set
} Types;
typedef Types** TypesHdl;

//  LabVIEW string utilities
void LV_str_cp(LStrHandle LV_string, char* c_str)
{
    DSSetHandleSize(LV_string, sizeof(int) + (strlen(c_str) + 1) * sizeof(char));
    (*LV_string)->cnt = strlen(c_str);
    strcpy((char*)(*LV_string)->str, c_str);
}
void LV_str_cp(LStrHandle LV_string, string c_str)
{
    DSSetHandleSize(LV_string, sizeof(int) + (c_str.length() + 1) * sizeof(char));
    (*LV_string)->cnt = c_str.length();
    strcpy((char*)(*LV_string)->str, c_str.c_str());
}
void LV_str_cat(LStrHandle LV_string, char* c_str)
{
    int n = (*LV_string)->cnt;
    DSSetHandleSize(LV_string, n + sizeof(int) + (strlen(c_str) + 1) * sizeof(char));
    (*LV_string)->cnt = n + strlen(c_str);
    strcpy((char*)(*LV_string)->str + n, c_str);
}
void LV_strncpy(LStrHandle LV_string, char* c_str, int size)
{
    DSSetHandleSize(LV_string, sizeof(int) + size * sizeof(char));
    (*LV_string)->cnt = size;
    strncpy((char*)(*LV_string)->str, c_str, size);
}

#endif

#ifdef MYAPI
#include "mysql_connection.h"
#include "/usr/include/mysql/mysql.h"
#endif
#ifdef MYCPPAPI
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

#define MAGIC 0x13131313  //  doesn't necessarily need to be unique to this, specific library
//  the odds another library class will be exactly the same length are low

class LvDbLib {       // LabVIEW MySQL Database class
public:
    uint canary_begin = MAGIC; //  check for buffer overrun and that we didn't delete object
    int errnum;       // error number
    string errstr;    // error description
    string errdata;   // data precipitating error
    string SQLstate;  // SQL state
    uint16_t type;    // RDMS type, see enum db_type
    int StrBufLen;    // initialize to 256, set to zero (0) for blobs and indeterminate length string results

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
            errnum = -1; errstr = "Unsupported RDBMS";
            break;
        }

        type = t; errnum = 0; errstr = "SUCCESS";
        if (ConnectionString.length() < 1) { errnum = -1; errstr = "Connection string may not be blank"; }
        else {
            switch (type)
            {
            case NULL:
                break;

#ifdef ODBCAPI
            case ODBC:
            case SqlServer:
                SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &(api.odbc.hEnv));
                SQLSetEnvAttr(api.odbc.hEnv, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
                SQLAllocHandle(SQL_HANDLE_DBC, api.odbc.hEnv, &(api.odbc.hDbc));
                SQLRETURN ret;
                ret = SQLDriverConnect(api.odbc.hDbc, NULL, (SQLCHAR*)ConnectionString.c_str(), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_COMPLETE);
                if (ret == SQL_ERROR) ODBC_ERROR(SQL_HANDLE_DBC, api.odbc.hDbc, ConnectionString);
                break;
#endif

#ifdef MYAPI
            case MySQL:
                if ((api.my.con = mysql_init(NULL)) == NULL)
                {
                    errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); break;
                }
                if (mysql_real_connect(api.my.con, ConnectionString.c_str(),
                    user.c_str(), pw.c_str(), db.c_str(), 0, "/run/mysql/mysql.sock", 0) == NULL)
                {
                    errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);
                }
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
                errnum = -1; errstr = "Unsupported RDBMS";
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
            return Execute("USE " + schema);
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

    int Query(string query) {  //  run query against connection and put results in res
        errnum = -1; errdata = query;
        if (query.length() < 1) { errstr = "Query string may not be blank"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
#if 0
            if (mysql_real_query(api.my.con, query.c_str(), query.length()))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;
            }
            if ((api.my.query_results = mysql_store_result(api.my.con)) == NULL)
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;
            }

            errnum = 0; errdata = ""; return mysql_affected_rows(api.my.con);
#else
            if (!(api.my.stmt = mysql_stmt_init(api.my.con))) { errnum = -1; errstr = "Out of memory"; return -1; }
            if (mysql_stmt_prepare(api.my.stmt, query.c_str(), query.length())) //  Prepare statement
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
            int param_count; param_count = mysql_stmt_param_count(api.my.stmt); api.my.bind = new MYSQL_BIND[param_count];
            if (mysql_stmt_execute(api.my.stmt))                                //  Execute
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return -1;}
            if (mysql_stmt_bind_param(api.my.stmt, api.my.bind))                //  bind results
                {errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); mysql_stmt_close(api.my.stmt); return -1;}
#endif
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
            errnum = -1; errstr = "Unsupported RDBMS";
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
        errnum = -1; errdata = query; int ans = 0;
        if (query.length() < 1) { errstr = "Query string may not be blank"; return -1; }
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
            if (mysql_real_query(api.my.con, query.c_str(), query.length()))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); ans = -1;
            }
            else
            {
                errnum = 0; errdata = ""; ans = mysql_affected_rows(api.my.con);
            }
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
        case SqlServer:
            if (!api.odbc.hDbc) { errnum = -1; errstr = "No DB connection"; return -1; }
            if (SQLAllocHandle(SQL_HANDLE_STMT, api.odbc.hDbc, &(api.odbc.hStmt)) == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "SQLAllocHandle"); return -1;}
            int rc; rc = SQLExecDirect(api.odbc.hStmt, (SQLCHAR*) query.c_str(), SQL_NTS);
            if (rc == SQL_ERROR) 
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query); ans = -1;}
            else SQLRowCount(api.odbc.hStmt, (SQLLEN*) &ans); 
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
#define CASE(xTD, cType) case  xTD:\
          union {cType f; char c[sizeof(cType)];} xTD;\
          xTD.f = in_data[i].xTD;\
          (**results).elt[row * cols + i] = (LStrHandle) DSNewHandle(sizeof(int32) + sizeof(cType));\
          LV_strncpy((**results).elt[row * cols + i], xTD.c, sizeof(cType));

        case MySQL:
            if (api.my.con == NULL) { errstr = "Connection closed"; return -1; }
            api.my.stmt = mysql_stmt_init(api.my.con);
            if (api.my.stmt == NULL) { errstr = "Out of memory"; return -1; }
            if (mysql_stmt_prepare(api.my.stmt, query.c_str(), query.length()))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); mysql_stmt_close(api.my.stmt); return -1;
            }
            MYSQL_BIND* bind; bind = new MYSQL_BIND[cols];

            for (j = 0; j < rows; j++)
            {
                for (i = 0; i < cols; i++)
                {
                    string val = v[j * cols + i];
                    switch (ColsTD[i])
                    {
                    case I8:
                    case U8:
                    case Boolean:
                        union { u_char f; char c[sizeof(char)]; } U8_;
                        U8_.c[0] = val[0];
                        bind[i].buffer_type = MYSQL_TYPE_TINY; bind[i].buffer = (char*)&U8_.f;
                        bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length
                        break;
                    case I16:
                    case U16:
                        union { short f; char c[sizeof(short)]; } I16_;
                        memcpy(I16_.c, val.c_str(), sizeof(short));
                        bind[i].buffer_type = MYSQL_TYPE_SHORT; bind[i].buffer = (char*)&I16_.f;
                        bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length
                        break;
                        break;
                    case I32:
                    case U32:
                        union { int f; char c[sizeof(int)]; } I32_;
                        memcpy(I32_.c, val.c_str(), sizeof(int));
                        bind[i].buffer_type = MYSQL_TYPE_LONG; bind[i].buffer = (char*)&I32_.f;
                        bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length
                        break;
                    case SGL:
                        union { float f; char c[sizeof(float)]; } SGL_;
                        memcpy(SGL_.c, val.c_str(), sizeof(float));
                        bind[i].buffer_type = MYSQL_TYPE_FLOAT; bind[i].buffer = (char*)&SGL_.f;
                        bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length
                        break;
                    case DBL:
                        union { double f; char c[sizeof(double)]; } DBL_;
                        memcpy(DBL_.c, val.c_str(), sizeof(double));
                        bind[i].buffer_type = MYSQL_TYPE_DOUBLE; bind[i].buffer = (char*)&DBL_.f;
                        bind[i].is_null = 0; bind[i].length = 0;  //  numerics don't need length
                        break;
                    case String:
                        char* str_data; str_data = (char*)val.c_str();
                        long unsigned str_length; str_length = val.length();
                        bind[i].buffer_type = MYSQL_TYPE_STRING; bind[i].buffer = (char*)str_data;
                        bind[i].buffer_length = str_length; bind[i].is_null = 0; bind[i].length = &str_length;
                        break;
                    default:
                        {errstr = "Data type (" + to_string(ColsTD[i]) + ") not supported"; return -1; }
                        break;
                    }
                }
                if ((errnum = mysql_stmt_bind_param(api.my.stmt, bind)) != 0)
                {
                    errstr = mysql_error(api.my.con); mysql_stmt_close(api.my.stmt); return -1;
                }
                if (mysql_stmt_execute(api.my.stmt) != 0)
                {
                    errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con);
                    mysql_stmt_close(api.my.stmt); return -1;
                }
            }
            ans = j; mysql_stmt_close(api.my.stmt); delete bind;
            {errno = 0; errstr = "SUCCESS"; return ans; }
            break;
#undef CASE
#endif

#ifdef ODBCAPI
#define CASE(LVt, SQLt, SQLCt, Ct) \
    case LVt:\
        union { Ct f; char c[sizeof(Ct)]; } LVt ## _;\
        std::memcpy(LVt ## _.c, val.c_str(), sizeof(Ct));\
        rc = SQLBindParameter(api.odbc.hStmt, i + 1, SQL_PARAM_INPUT, \
            SQLt, SQLCt, sizeof(Ct), 0, &LVt ## _.f, 0, &cbValue);\
        if (rc == SQL_ERROR) \
            {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query);\
             SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}

        case ODBC:
        case SqlServer:
            if (api.odbc.hDbc == NULL) { errno = -1; errstr = "Connection closed"; return -1; }
            if (SQLAllocHandle(SQL_HANDLE_STMT, api.odbc.hDbc, &(api.odbc.hStmt)) == SQL_ERROR)
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, "SQLAllocHandle"); return -1;}
            int rc; rc = SQLPrepare(api.odbc.hStmt, (SQLCHAR*) query.c_str(), SQL_NTS);
            if (rc == SQL_ERROR) 
                {ODBC_ERROR(SQL_HANDLE_STMT, api.odbc.hStmt, query); SQLFreeHandle(SQL_HANDLE_STMT, api.odbc.hStmt); return -1;}
            for (j = 0; j < rows; j++)
            {
                for (i = 0; i < cols; i++)
                {
                    SQLINTEGER cbValue; cbValue = SQL_NTS;
                    string val = v[j * cols + i];
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
                            rc = SQLBindParameter(api.odbc.hStmt, i + 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, 50, 0, (char*) val.c_str(), val.length(), &cbValue);
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
            if (api.mycpp.con == NULL) { errno = -1; errstr = "Connection closed"; return -1; }
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

    bool GetResults(int rows, int cols, TypesHdl types, ResultSetHdl results) {  //  return results as LV flattened strings
        bool ans = false;
        errnum = 0; errdata = "";
        int row = 0; //  row number

        DSSetHandleSize(results, sizeof(int32) * 2 + rows * cols * sizeof(LStrHandle));
        (**results).dimSizes[0] = rows; (**results).dimSizes[1] = cols;
        switch (type)
        {
        case NULL:
            break;

#ifdef MYAPI
        case MySQL:
#if 0
            MYSQL_ROW r; int* lengths;  //  typedef char **MYSQL_ROW; /* return data as array of strings */
            while ((r = mysql_fetch_row(api.my.query_results)) != NULL)
            {
                lengths = (int*)mysql_fetch_lengths(api.my.query_results);
                for (int i = 0; i < cols; i++) {
                    switch ((**types).TypeDescriptor[i])
                    {
                    case  Boolean:  //  any of these numeric type might overflow, that's what we'd like to catch
                    case  U8:
                        union { u_char f; char c[sizeof(char)]; } U8;
                        U8.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                        LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                        break;
                    case  I8:
                        union { char f; char c[sizeof(char)]; } I8;
                        I8.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                        LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                        break;
                    case  U16:
                        union { u_int16_t f; char c[sizeof(u_int16_t)]; } U16;
                        U16.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(u_int16_t));
                        LV_strncpy((**results).elt[row * cols + i], U16.c, sizeof(u_int16_t));
                        break;
                    case  I16:
                        union { u_int16_t f; char c[sizeof(u_int16_t)]; } I16;
                        I16.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(int16_t));
                        LV_strncpy((**results).elt[row * cols + i], I16.c, sizeof(int16_t));
                    case  U32:
                        union { u_int32_t f; char c[sizeof(u_int32_t)]; } U32;
                        U32.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(u_int32_t));
                        LV_strncpy((**results).elt[row * cols + i], U32.c, sizeof(u_int32_t));
                        break;
                    case  I32:
                        union { u_int32_t f; char c[sizeof(u_int32_t)]; } I32;
                        I32.f = stoi(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(int32_t));
                        LV_strncpy((**results).elt[row * cols + i], I32.c, sizeof(int32_t));
                        break;
                    case  SGL:
                        union { float f; char c[sizeof(float)]; } SGL;
                        SGL.f = stof(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(float));
                        LV_strncpy((**results).elt[row * cols + i], SGL.c, sizeof(float));
                        break;
                    case  DBL:
                        union { double f; char c[sizeof(double)]; } DBL;
                        DBL.f = stod(string(r[i], lengths[i]));
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(double));
                        LV_strncpy((**results).elt[row * cols + i], DBL.c, sizeof(double));
                        break;
                    case  String:
                    default:  //  since we have length, we can trivially handle binary data/BLOBS
                        (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(0);
                        LV_str_cp((**results).elt[row * cols + i], string(r[i], lengths[i]));
                        break;
                    }
                }
                row++;
            }
            mysql_free_result(api.my.query_results);
#else
            if (!(api.my.query_results = mysql_stmt_result_metadata(api.my.stmt))) //  Fetch result set meta information
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return false;
            }
            if (cols != mysql_num_fields(api.my.query_results))
            {
                errnum = -1; errstr = "Data column number mismatch"; return false;
            }

            /* Fetch result set meta information */
            MYSQL_FIELD* fields; fields = mysql_fetch_fields(api.my.query_results);

            unsigned long* length; length = (long unsigned int*) new unsigned int[cols];
            my_bool* error, * is_null;
            error = (my_bool*) new my_bool[cols]; is_null = (my_bool*) new my_bool[cols];
            union _in_data
            {
                int8_t  I8;   u_int8_t  U8;
                int16_t I16;  u_int16_t U16;
                int32_t I32;  u_int32_t U32;
                float   SGL;  double DBL;
                char* str;
            } *in_data; in_data = (_in_data*) new _in_data[cols];

            for (int i = 0; i < cols; i++) {  //  bind buffers
                api.my.bind[i].is_null = &is_null[i]; api.my.bind[i].error = &error[i];
                api.my.bind[i].length = &length[i]; api.my.bind[i].buffer_type = fields[i].type;
                switch (fields[i].type)
                {
                case MYSQL_TYPE_TINY ... MYSQL_TYPE_DOUBLE:
                    api.my.bind[i].buffer = (char*)&(in_data[i]);
                    break;
                case MYSQL_TYPE_TINY_BLOB ... MYSQL_TYPE_STRING:
                    break;
                    in_data[i].str = (char*) new char[StrBufLen];
                    api.my.bind[i].buffer = (char*)in_data[i].str;
                    api.my.bind[i].buffer_length = StrBufLen;
                default:
                    delete length; delete is_null; delete error; delete api.my.bind; delete in_data;
                    errnum = -1; errstr = "Unsupported MySQL type: " + to_string(fields[i].type);
                    mysql_free_result(api.my.query_results); mysql_stmt_close(api.my.stmt);
                    return false;
                    break;
                }
            }
            if (mysql_stmt_bind_result(api.my.stmt, api.my.bind))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return false;
            }

            while (!mysql_stmt_fetch(api.my.stmt)) {  //  Fetch all rows
                row++;
                for (int i = 0; i < cols; i++)
                {
                    // NOTE:  NULL DB results map only to LStr NULL string -> LStr NULL variant
                    //        The only LV TD that has a something we can use for NULL is float/double (NaN)
                    //        In the re-conversion to Numeric/String, NULL Variants should be an error state

                    if (!is_null[i])
                    {
                        switch ((**types).TypeDescriptor[i])
                        {
                        case  Boolean:  //  any of these numeric type might overflow, that's what we'd like to catch
                        case  U8:
                            union { u_char f; char c[sizeof(char)]; } U8;
                            U8.f = in_data[i].U8;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                            LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                            break;
                        case  I8:
                            union { char f; char c[sizeof(char)]; } I8;
                            I8.f = in_data[i].I8;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                            LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                            break;
                            CASE(U16, u_int16_t)
                                break;
                            CASE(I16, int16_t)
                                break;
                        case  U32:
                            union { u_int32_t f; char c[sizeof(u_int32_t)]; } U32;
                            U32.f = in_data[i].U32;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(u_int32_t));
                            LV_strncpy((**results).elt[row * cols + i], U32.c, sizeof(u_int32_t));
                            break;
                        case  I32:
                            union { int32 f; char c[sizeof(int32)]; } I32;
                            I32.f = in_data[i].I32;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(int32));
                            LV_strncpy((**results).elt[row * cols + i], I32.c, sizeof(int32));
                            break;
                        case  SGL:
                            union { float f; char c[sizeof(float)]; } SGL;
                            SGL.f = in_data[i].SGL;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(float));
                            LV_strncpy((**results).elt[row * cols + i], SGL.c, sizeof(float));
                            break;
                        case  DBL:
                            union { double f; char c[sizeof(double)]; } DBL;
                            DBL.f = in_data[i].DBL;
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(double));
                            LV_strncpy((**results).elt[row * cols + i], DBL.c, sizeof(double));
                            break;
                        case  String:
                        default:
                            (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(0);
                            if (length[i] > StrBufLen)
                            {
                                // char* data; data = new char[length[i]];
                                delete[] in_data[i].str; in_data[i].str = new char[length[i]];
                                int retval; if ((retval = mysql_stmt_fetch_column(api.my.stmt, api.my.bind, i, 0)) != 0)
                                {
                                    errnum = retval; errstr = mysql_error(api.my.con); return false;
                                }
                            }
                            else
                            {
                                LV_str_cp((**results).elt[row * cols + i], string(in_data[i].str, length[i]));
                            }
                            break;
                        }
                    }
                }
            }
            for (int i = 0; i < cols; i++)   //  bind buffers
                if ((**types).TypeDescriptor[i] == String) delete in_data[i].str;

            delete length; delete is_null; delete error; delete api.my.bind; delete in_data;
            mysql_free_result(api.my.query_results);
            if (mysql_stmt_close(api.my.stmt))
            {
                errnum = mysql_errno(api.my.con); errstr = mysql_error(api.my.con); return false;
            }
#endif
            errnum = 0; errdata = ""; errstr = "SUCCESS"; ans = true;
            break;
#endif

#ifdef ODBCAPI
        case ODBC:
            break;
#endif

#ifdef MYCPPAPI
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
                            case  U8:
                                union { u_char f; char c[sizeof(char)]; } U8;
                                U8.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                                LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                                break;
                            case  I8:
                                union { char f; char c[sizeof(char)]; } I8;
                                I8.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(char));
                                LV_strncpy((**results).elt[row * cols + i], U8.c, sizeof(char));
                                break;
                            case  U16:
                                union { u_int16_t f; char c[sizeof(u_int16_t)]; } U16;
                                U16.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(u_int16_t));
                                LV_strncpy((**results).elt[row * cols + i], U16.c, sizeof(u_int16_t));
                                break;
                            case  I16:
                                union { u_int16_t f; char c[sizeof(u_int16_t)]; } I16;
                                I16.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(int16_t));
                                LV_strncpy((**results).elt[row * cols + i], I16.c, sizeof(int16_t));
                            case  U32:
                                union { u_int32_t f; char c[sizeof(u_int32_t)]; } U32;
                                U32.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(u_int32_t));
                                LV_strncpy((**results).elt[row * cols + i], U32.c, sizeof(u_int32_t));
                                break;
                            case  I32:
                                union { u_int32_t f; char c[sizeof(u_int32_t)]; } I32;
                                I32.f = res->getInt(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(int32_t));
                                LV_strncpy((**results).elt[row * cols + i], I32.c, sizeof(int32_t));
                                break;
                            case  SGL:
                                union { float f; char c[sizeof(float)]; } SGL;
                                SGL.f = res->getDouble(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(float));
                                LV_strncpy((**results).elt[row * cols + i], SGL.c, sizeof(float));
                                break;
                            case  DBL:
                                union { double f; char c[sizeof(double)]; } DBL;
                                DBL.f = res->getDouble(i + 1);
                                (**results).elt[row * cols + i] = (LStrHandle)DSNewHandle(sizeof(int32) + sizeof(double));
                                LV_strncpy((**results).elt[row * cols + i], DBL.c, sizeof(double));
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
#endif

        default:
            errnum = -1; errstr = "Unsupported RDBMS"; ans = false;
            break;
        }
        return ans;
    }

    uint canary_end = MAGIC;  //  check for buffer overrun and that we didn't delete object
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

#define LStrString(A) string((char*) (*A)->str, (*A)->cnt)
extern "C" {  //  functions to be called from LabVIEW.  'extern "C"' is necessary to prevent overloading name mangling

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

    int SetBufLen(LvDbLib* LvDbObj, int len) { //  set string buffer length, set to zero (0) for blobs and other large strings for dynamic allocation
        if (!IsObj(LvDbObj)) return -1;
        LvDbObj->StrBufLen = len;
        return 0;
    }

    int Execute(LvDbLib* LvDbObj, LStrHandle query) { //  run query against connection and return num rows affected
        if (!IsObj(LvDbObj)) return -1;
        return LvDbObj->Execute(LStrString(query));
    }

    int UpdatePrepared(LvDbLib* LvDbObj, LStrHandle query, DataSetHdl data, uint16_t ColsTD[]) { //  run prepared statement and return num rows affected
        if (!IsObj(LvDbObj)) return -1;
        int rows = (**data).dimSizes[0]; int cols = (**data).dimSizes[1];
        string* vals = new string[rows * cols];
        for (int j = 0; j < rows; j++)
            for (int i = 0; i < cols; i++) {
                LStrHandle s = (**data).elt[j * cols + i];
                vals[j * cols + i] = LStrString(s);
            }
        return LvDbObj->UpdatePrepared(LStrString(query), vals, rows, cols, ColsTD);
    }

    int Query(LvDbLib* LvDbObj, LStrHandle query, TypesHdl types, ResultSetHdl results) { //  run query against connection and return result set in flattened strings
        int rows, cols = (**types).dimSize; if (cols == 0) return 0;  //  number of columns, return if no data columns requested  
        if (!IsObj(LvDbObj)) return -1;
        if ((rows = LvDbObj->Query(LStrString(query))) < 0) return -1; //  std::string version of SQL query
        if (!LvDbObj->GetResults(rows, cols, types, results)) return -1;
        else return rows;
    }

    int CloseDB(LvDbLib* LvDbObj) { //  close DB connection and free memory
        if (LvDbObj == NULL) { ObjectErrStr = "NULL DB object"; ObjectErr = true; return -1; }
        if (!IsObj(LvDbObj)) return -1;
        myObjs.remove(LvDbObj); delete LvDbObj; return 0;
    }

    int Type(LvDbLib* LvDbObj) { //  close DB connection and free memory
        if (LvDbObj == NULL) { ObjectErrStr = "NULL DB object"; ObjectErr = true; return -1; }
        if (!IsObj(LvDbObj)) return -1;
        return LvDbObj->type;
    }

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

}
