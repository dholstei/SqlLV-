// RDMS type
enum db_type
{
//   NULL      = 0x00, // Void
  ODBC        = 0x01, // ODBC
  MySQL       = 0x02, // MySQL C Connector
  Oracle      = 0x03, // Oracle
  SqlServer   = 0x04, // SQL Server
  MySQLpp     = 0x05  // MySQL Connector/C++
};