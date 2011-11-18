#!/bin/bash

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


echo "help $1
quit" | ../../../../bin/accumulo shell -u foo -p foo --fake 2>&1 >/dev/null | sed -E "s/  +/  /g" | perl -ne 'use Text::Wrap; $Text::Wrap::columns=82; $Text::Wrap::break="[ ]"; print wrap("","              ",$_);' | gawk '{print "   ",$0}'
