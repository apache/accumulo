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
echo "help -np
quit" | \
../../../../bin/accumulo shell -u foo -p foo --fake | \
#head -11 | \
gawk 'BEGIN{count = 0}{if(count > 9 && substr($0,0,4) != "foo@"){print "\\textbf{"$1"}\n"; system("./get_shell_command.sh \""$1"\""); print "";};count += 1}' |\
sed -e 's/_/\\_/g'
echo
echo "\\end{alltt}"

