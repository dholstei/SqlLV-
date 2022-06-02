##############################################################################
#
#                     Makefile for sql_LV.so
#
##############################################################################

# This variable contains the flags passed to cc.

CC = gcc -g
C++ = g++ -g
F77 = g77
MYSQL_LIB=/usr/lib/libmariadb.so.3
#MYSQLCPP_LIB=/home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/lib/libmysqlcppconn.so
MYSQLCPP_LIB=/home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/lib/libmysqlcppconn-static.a
LABVIEW_LIB=/usr/local/lv71/AppLibs/liblvrt.so.7.1
ODBC_LIB=/usr/lib/libodbc.so
SDL_LIB=/usr/lib/libSDL2-2.0.so.0
ifeq ($(ODBC),1)
  CFLAGS = -m32 -fPIC -DHAVE_ODBC -Di686
  LIBS = $(MYSQL_LIB) $(LABVIEW_LIB) $(ODBC_LIB)
else
  CFLAGS = -m32 -fPIC -Di686
  CPPFLAGS = -m32 -D_GLIBCXX_USE_CXX11_ABI=0 
  LIBS = $(MYSQL_LIB) $(LABVIEW_LIB) $(SDL_LIB)
endif
OBJ = o
SUFFIX = so
INCLUDES = -I/usr/include/mysql/ -I/usr/local/lv71/cintools\
 -I/home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/include/jdbc/
#  DLLFLAGS = -shared -W1,-soname,$@
DLLFLAGS = -shared -m32
.SUFFIXES:
.SUFFIXES: .c .cpp .o .so

OBJECTS = sql_LV.$(OBJ)



# This variable points to the directory containing the X libraries.

LIBDIR = /usr/X11R6/lib

.c:
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

.c.o: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

.cpp.o: 
	$(C++) $(INCLUDES) -c $(CPPFLAGS) $<

.c.obj: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

all:    sql_LV.$(SUFFIX)

sql_LVpp:	sql_LVpp.o
	$(C++) $(CPPFLAGS) -o $@ $? $(MYSQLCPP_LIB) -ldl -lpthread -lresolv -lssl -lcrypto

sql_LVpp.so: sql_LVpp.cpp
	$(C++) $(CPPFLAGS) -DLIB -shared -fPIC -o $@ $? $(INCLUDES) $(MYSQLCPP_LIB) -ldl -lpthread -lresolv -lssl -lcrypto

sql_LV.so:    $(OBJECTS)
	$(CC) $(DLLFLAGS) -o $@ $(OBJECTS) $(LIBS)

sql_LV.dll:    $(OBJECTS)
	$(LINKER) $(DLLFLAGS) $(OBJECTS) $(LIBS)

clean:
	 rm -f\
	 sql_LV.$(SUFFIX) *.$(OBJ) sql_LVpp

dist:
	 tar cvfz sql_LV.tgz *.c *.h Makefile *.llb

test:    sql_LV.$(SUFFIX)
	 $(CC) test.c -o test sql_LV.$(SUFFIX)

