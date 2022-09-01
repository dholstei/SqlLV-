##############################################################################
#
#                     Makefile for sql_LV.so
#
##############################################################################

# This variable contains the flags passed to cc.

CC = gcc
C++ = g++

OBJ = o
SUFFIX = so

ifeq ($(X64), 1)	#	64-bit, release
CFLAGS = -g -fPIC
CXXFLAGS = -g -std=gnu++17 -lstdc++ # -DOpSystem=kLinux

INCLUDES := $(INCLUDES)  -I/usr/local/natinst/LabVIEW-2022-64/cintools/
LIBS := $(LIBS) /usr/local/lib64/liblvrt.so

else				#	32-bit, debug
CFLAGS = -g -m32 -fPIC -Di686
CXXFLAGS = -g -m32 -std=gnu++17 -lstdc++

INCLUDES := $(INCLUDES)  -I/usr/local/lv71/cintools
LIBS := $(LIBS) /usr/local/lv71/AppLibs/liblvrt.so.7.1
endif

ifeq ($(ODBC),1)	# UnixODBC API
	CXXFLAGS := $(CXXFLAGS) -DODBCAPI
#	 INCLUDES := $(INCLUDES) -I/usr/include/
	LIBS := $(LIBS) /usr/lib/libodbc.so
endif

ifeq ($(MySQL),1)	# MySQL API
	CXXFLAGS := $(CXXFLAGS) -DMYAPI
	INCLUDES := $(INCLUDES) -I/usr/include/mysql/
	LIBS := $(LIBS) /usr/lib/libmariadb.so.3
	# LIBS := $(LIBS) /home/danny/src/mysql-connector-c-6.1.11-linux-glibc2.12-i686/lib/libmysqlclient.a
	# LIBS := $(LIBS) /usr/lib64/libmariadb.so.3
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

OBJECTS = sql_LVpp.$(OBJ)

.c:
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

.c.o: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

.cpp.o: 
	$(C++) $(INCLUDES) -c $(CXXFLAGS) $<

.c.obj: 
	$(CC) $(INCLUDES) -c $(CFLAGS) $<

all:    sql_LVpp.so

sql_LVpp.so: sql_LVpp.cpp
	$(C++) $(CXXFLAGS) -shared -fPIC -o $@ $? $(INCLUDES) $(LIBS)\
	 -ldl -lpthread -lresolv -lssl -lcrypto

# sql_LVpp.so:    $(OBJECTS)
# 	$(CC) $(DLLFLAGS) -o $@ $(OBJECTS) $(LIBS)

sql_LV.dll:    $(OBJECTS)
	$(LINKER) $(DLLFLAGS) $(OBJECTS) $(LIBS)

clean:
	 rm -f\
	 sql_LVpp.$(SUFFIX) *.$(OBJ)

dist:
	 tar cvfz sql_LV.tgz *.c *.h Makefile *.llb

test:    sql_LVpp.so
	 $(CC) $(CXXFLAGS) test.cpp -o test $<

