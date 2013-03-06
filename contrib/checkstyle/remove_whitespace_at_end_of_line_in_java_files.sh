#! /usr/bin/env bash

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

# One of the rules in the checkstyle tool is that java files should not
# have whitespace at the end of lines.

# The commands below:
#   1. find all java files (find ../.. -type f -name *.java)
#   2. remove whitespace (sed 's/[ ]\t]*$//')

for file in $(find ../.. -type f -name *.java); do
  echo "Processing $file";
  cat $file | sed 's/[ \t]*$//' > $file.new; 
  rm $file
  mv $file.new $file
done

