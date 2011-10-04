LIB=MLock
SRCS=$(filter-out test.cc,$(wildcard *.cc))
HDRS=$(wildcard *.h) accumulo_server_tabletserver_$(LIB).h
CXX=g++

ifeq ($(shell uname),Linux)
LIBS:= lib$(LIB)-Linux-amd64-64.so lib$(LIB)-Linux-i386-32.so
CXXFLAGS=-g -fPIC -shared -O2 -fno-omit-frame-pointer -fno-strict-aliasing -Wall  -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include
endif

ifeq ($(shell uname),Darwin)
LIBS:= lib$(LIB)-Mac_OS_X-x86_64-64.jnilib
CXXFLAGS=-m64 -dynamiclib -O3 -I/System/Library/Frameworks/JavaVM.framework/Headers
endif

INSTALL_DIR=../../../../../../lib/native/mlock
INSTALLED_LIBS=$(patsubst %,$(INSTALL_DIR)/%,$(LIBS))

all : $(INSTALLED_LIBS)

clean:
	rm -f $(INSTALLED_LIBS) $(LIBS)

accumulo_server_tabletserver_$(LIB).h : ../../java/org/apache/accumulo/server/tabletserver/$(LIB).java
	javah -classpath ../../../../../../lib/accumulo-server-1.3.5-incubating-SNAPSHOT.jar org.apache.accumulo.server.tabletserver.$(LIB)

$(INSTALLED_LIBS) : $(INSTALL_DIR) $(LIBS)
	cp $(LIBS) $(INSTALL_DIR)

$(INSTALL_DIR):
	mkdir $@

lib$(LIB)-Linux-amd64-64.so : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m64 -o $@ $(SRCS) 

lib$(LIB)-Linux-i386-32.so : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m32 -o $@ $(SRCS) 

lib$(LIB)-Mac_OS_X-x86_64-64.jnilib : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m64 -o $@ $(SRCS)

