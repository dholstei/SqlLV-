##############################################################################
#
#                     Makefile for sql_LV.so
#
##############################################################################

# This variable contains the flags passed to cc.

CC = gcc -g
C++ = g++ -g
CFLAGS = -m32 -fPIC -Di686
CXXFLAGS = -m32 -std=gnu++17 -lstdc++

# LabVIEW
INCLUDES := $(INCLUDES)  -I/usr/local/lv71/cintools
LIBS := $(LIBS) /usr/local/lv71/AppLibs/liblvrt.so.7.1

ifeq ($(ODBC),1)	# UnixODBC API
	CXXFLAGS := $(CXXFLAGS) -DODBCAPI
#	 INCLUDES := $(INCLUDES) -I/usr/include/
	LIBS := $(LIBS) /usr/lib/libodbc.so
endif

ifeq ($(MySQL),1)	# MySQL API
	CXXFLAGS := $(CXXFLAGS) -DMYAPI
	INCLUDES := $(INCLUDES) -I/usr/include/mysql/
	LIBS := $(LIBS) /usr/lib/libmariadb.so.3
endif

ifeq ($(MySQLCPP),1)	# MySQL C++/Connector
	CXXFLAGS := $(CXXFLAGS) -DMYCPPAPI
	INCLUDES := $(INCLUDES)  -I/home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/include/jdbc/
	LIBS := $(LIBS) /home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/lib/libmysqlcppconn-static.a
#	MYSQLCPP_LIB=/home/danny/src/mysql-connector-c++-8.0.28-linux-glibc2.12-x86-32bit/lib/libmysqlcppconn.so
	CXXFLAGS := $(CXXFLAGS) -D_GLIBCXX_USE_CXX11_ABI=0 
endif

SDL_LIB=/usr/lib/libSDL2-2.0.so.0

#  DLLFLAGS = -shared -W1,-soname,$@
DLLFLAGS = -shared -m32
.SUFFIXES:
.SUFFIXES: .c .cpp .o .so

OBJECTS = sql_LV.$(OBJ)

.c:
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

.c.o: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

.cpp.o: 
	$(C++) $(INCLUDES) -c $(CXXFLAGS) $<

.c.obj: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

all:    sql_LVpp.so

sql_LVpp:	sql_LVpp.o
	$(C++) $(CXXFLAGS) -o $@ $? $(MYSQLCPP_LIB) -ldl -lpthread -lresolv -lssl -lcrypto

sql_LVpp.so: sql_LVpp.cpp
	$(C++) $(CXXFLAGS) -shared -fPIC -o $@ $? $(INCLUDES) $(LIBS)\
	 -ldl -lpthread -lresolv -lssl -lcrypto

sql_LV.so:    $(OBJECTS)
	$(CC) $(DLLFLAGS) -o $@ $(OBJECTS) $(LIBS)

sql_LV.dll:    $(OBJECTS)
	$(LINKER) $(DLLFLAGS) $(OBJECTS) $(LIBS)

clean:
	 rm -f\
	 sql_LV.$(SUFFIX) *.$(OBJ) sql_LVpp

dist:
	 tar cvfz sql_LV.tgz *.c *.h Makefile *.llb

test:    sql_LVpp.so
	 $(CC) $(CXXFLAGS) test.cpp -o test $<

