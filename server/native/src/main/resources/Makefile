#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

SRCS=$(wildcard nativeMap/*.cc)
HDRS=$(wildcard nativeMap/*.h) $(wildcard javah/*.h)
TESTSRCS=$(wildcard testNativeMap/*.cc)
CXX=g++
MAVERICKFLAGS=
USERFLAGS=$(shell env | grep "^USERFLAGS=" | cut -d= -f2)

ifneq (,$(OUTPUT_DIR))
	OUTPUT_DIR_ESCAPED=$(shell echo '$(OUTPUT_DIR)' | sed 's/ /\\ /g' | sed 's/\([()]\)/\\\1/g')
endif

ifeq ($(shell uname),Linux)
	JAVA_HOME=$(shell env | grep "^JAVA_HOME=" | cut -d= -f2)
	ifeq ($(strip $(JAVA_HOME)),)
		JAVA_HOME=$(shell dirname "$$(dirname "$$(readlink -e "$$(which javac)")")")
	endif
	NATIVE_LIB := libaccumulo.so
	CXXFLAGS=-g -fPIC -shared -O3 -Wall -I'$(JAVA_HOME)'/include -I'$(JAVA_HOME)'/include/linux -Ijavah $(USERFLAGS)
endif

ifeq ($(shell uname),Darwin)
	JAVA_HOME=$(shell env | grep "^JAVA_HOME=" | cut -d= -f2)
	ifeq ($(strip $(JAVA_HOME)),)
		JAVA_HOME=$(shell /usr/libexec/java_home)
	endif
	NATIVE_LIB:= libaccumulo.dylib
ifneq (,$(findstring 10.9,$(shell sw_vers -productVersion)))
	MAVERICKFLAGS=-stdlib=libstdc++
endif
	CXXFLAGS=-dynamiclib -undefined dynamic_lookup -O3 -I/System/Library/Frameworks/JavaVM.framework/Headers -I'$(JAVA_HOME)'/include -I'$(JAVA_HOME)'/include/darwin -Ijavah $(USERFLAGS) $(MAVERICK_FLAGS)
endif

# escape space and parens in path for wildcard
JAVA_HOME_ESCAPED=$(shell echo '$(JAVA_HOME)' | sed 's/ /\\ /g' | sed 's/\([()]\)/\\\1/g')
ifeq (,$(wildcard $(JAVA_HOME_ESCAPED)/bin/javac))
$(error "JAVA_HOME does not point to a JDK. Exiting...")
endif

all : $(NATIVE_LIB)

$(NATIVE_LIB) : $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) -o $@ $(SRCS)

test : $(NATIVE_LIB) testJavaHome runTests

testJavaHome :
	@echo 'JAVA_HOME is $(JAVA_HOME)'

runTests : $(NATIVE_LIB) $(TESTSRCS) $(OUTPUT_DIR_ESCAPED)
	$(CXX) -g -Wall -I/System/Library/Frameworks/JavaVM.framework/Headers -I'$(JAVA_HOME)'/include -I'$(JAVA_HOME)'/include/linux -I'$(JAVA_HOME)'/include/darwin -InativeMap -o $@ $(TESTSRCS) $(NATIVE_LIB) $(MAVERICK_FLAGS)
	LD_LIBRARY_PATH=./ ./$@ 20 20 20 20 20 20 20 20 true
	if [ ! -z "$(OUTPUT_DIR)" ]; then cp '$(NATIVE_LIB)' "$(OUTPUT_DIR)"; fi

clean :
	rm -f '$(NATIVE_LIB)' runTests
