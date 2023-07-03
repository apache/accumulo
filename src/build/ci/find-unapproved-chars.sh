#! /usr/bin/env bash
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

# The purpose of this ci script is to ensure that a pull request doesn't
# unintentionally, or maliciously, add any new non-ASCII characters unless they
# are preapproved on the ALLOWED list or in known binary or resource files
NUM_EXPECTED=0
ALLOWED='©èö🐈三四五六八九十'

function findallnonascii() {
  # -P for perl matching, -o for only showing the match for counting occurrences not lines
  local opts='-Po'
  if [[ $1 == 'print' ]]; then
    # -P for perl matching, -H for always showing filenames, -n for showing line numbers
    opts='-PHn'
  fi
  find . -type f \
    -not -path '*/\.git/*' \
    -not -path '*/monitor/resources/external/*' \
    -not -path '*/tserver/src/test/resources/walog-from-14/*' \
    -not -regex '.*[.]\(png\|jar\|rf\|jceks\|walog\)$' \
    -exec grep "$opts" "[^[:ascii:]$ALLOWED]" {} +
}

function comparecounts() {
  local count
  count=$(findallnonascii | wc -l)
  if [[ $NUM_EXPECTED -ne $count ]]; then
    echo "Expected $NUM_EXPECTED, but found $count unapproved non-ASCII characters:"
    findallnonascii 'print'
    return 1
  fi
}

comparecounts && echo "Found exactly $NUM_EXPECTED unapproved non-ASCII characters, as expected"
