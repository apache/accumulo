#! /bin/bash

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


bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
cd $bin

if [ `which pdflatex | wc -l` -eq 0 ]; then
  echo 'Missing pdflatex command. Please install.' 1>&2
  exit 0;
fi

if [ -f ../../accumulo_user_manual.pdf ]
then
  if [ `find . -name '*.tex' -newer ../../accumulo_user_manual.pdf | wc -l` -eq 0 ] 
  then
   exit 0
  fi
fi

pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex 
pdflatex accumulo_user_manual.tex && (
find . -name '*.aux' -print | xargs rm -f 
rm -f *.log *.toc *.out
mv accumulo_user_manual.pdf ../..
)
