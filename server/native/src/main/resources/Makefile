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

SRCS=$(wildcard nativeMap/*.cc)
HDRS=$(wildcard nativeMap/*.h) $(wildcard javah/*.h)
CXX=g++

ifeq ($(shell uname),Linux)
	JAVA_HOME=$(shell dirname $$(dirname $$(readlink -ef $$(which javah))))
	NATIVE_LIB := libaccumulo.so
	CXXFLAGS=-g -fPIC -shared -O2 -fno-omit-frame-pointer -fno-strict-aliasing -Wall -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -Ijavah
endif

ifeq ($(shell uname),Darwin)
	JAVA_HOME=$(shell /usr/libexec/java_home)
	NATIVE_LIB:= libaccumulo.jnilib
	CXXFLAGS=-m64 -dynamiclib -O3 -I$(JAVA_HOME)/include -Ijavah
endif

all : $(NATIVE_LIB)

$(NATIVE_LIB) : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -m64 -o $@ $(SRCS) 

test : testJavaHome

testJavaHome :
	@echo JAVA_HOME is $(JAVA_HOME)

