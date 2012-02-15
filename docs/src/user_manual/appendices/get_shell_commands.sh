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


echo "% Licensed to the Apache Software Foundation (ASF) under one or more
% contributor license agreements.  See the NOTICE file distributed with
% this work for additional information regarding copyright ownership.
% The ASF licenses this file to You under the Apache License, Version 2.0
% (the \"License\"); you may not use this file except in compliance with
% the License.  You may obtain a copy of the License at
%
%     http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an \"AS IS\" BASIS,
% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% See the License for the specific language governing permissions and
% limitations under the License.
"

echo "\\chapter{Shell Commands}"
echo
echo "\\begin{alltt}"
../../../../bin/accumulo shell -u foo -p foo --fake -e "help -nw `../../../../bin/accumulo shell -u foo -p foo --fake -e "help -np -nw" | cut -d" " -f1 | perl -ne 'chomp; print; print " "'`" 2>&1 | perl -ne '$l = $_; if (/^usage: ([^ ]+)/) {print "\n\\textbf{$1}\n\n"} $l =~ s/\t/  /g; $l =~ s/  +/  /g; use Text::Wrap; $Text::Wrap::columns=89; $Text::Wrap::break="[ ]"; $Text::Wrap::unexpand=0; print wrap("    ","              ",$l);'
echo
echo "\\end{alltt}"

