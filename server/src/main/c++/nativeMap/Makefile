# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SRCS=$(filter-out test.cc,$(wildcard *.cc))
HDRS=$(wildcard *.h) org_apache_accumulo_server_tabletserver_NativeMap.h
CXX=g++

ifeq ($(shell uname),Linux)
  LIBS_32 := libNativeMap-Linux-i386-32.so
  LIBS_64 := libNativeMap-Linux-amd64-64.so
  
  ifneq ($(DARCH),)
    ifeq ($(DARCH),64)
      LIBS := $(LIBS_64)
    endif
    ifeq ($(DARCH),32)
      LIBS := $(LIBS_32)
    endif
    ifeq ($(DARCH),both)
      LIBS := $(LIBS_64) $(LIBS_32)
    endif
    ifeq ($(LIBS),)
      LIBS := $(LIBS_64) $(LIBS_32)
    endif
  else
    DARCH := $(shell getconf LONG_BIT)
    LIBS := $(LIBS_$(DARCH))
  endif
  
  CXXFLAGS=-g -fPIC -shared -O2 -fno-omit-frame-pointer -fno-strict-aliasing -Wall -I$(JAVA_HOME)/include/linux -I$(JAVA_HOME)/include
endif

ifeq ($(shell uname),Darwin)
  LIBS:= libNativeMap-Mac_OS_X-x86_64-64.jnilib
  CXXFLAGS=-m64 -dynamiclib -O3 -I/System/Library/Frameworks/JavaVM.framework/Headers
endif

INSTALL_DIR=../../../../../lib/native/map
INSTALLED_LIBS=$(patsubst %,$(INSTALL_DIR)/%,$(LIBS))

all : $(INSTALLED_LIBS)

clean:
	rm -f $(INSTALLED_LIBS) $(LIBS)

org_apache_accumulo_server_tabletserver_NativeMap.h :
	javah -classpath ../../../../../lib/accumulo-server.jar org.apache.accumulo.server.tabletserver.NativeMap

$(INSTALLED_LIBS) : $(INSTALL_DIR) $(LIBS)
	cp $(LIBS) $(INSTALL_DIR)

$(INSTALL_DIR):
	mkdir -p $@

libNativeMap-Linux-amd64-64.so : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m64 -o $@ $(SRCS) 

libNativeMap-Linux-i386-32.so : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m32 -o $@ $(SRCS) 

libNativeMap-Mac_OS_X-x86_64-64.jnilib : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m64 -o $@ $(SRCS)

