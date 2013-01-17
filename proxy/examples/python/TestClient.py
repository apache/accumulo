#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
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

import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from proxy import AccumuloProxy
from proxy.ttypes import *

transport = TSocket.TSocket('localhost', 42424)
transport = TTransport.TFramedTransport(transport)
protocol = TCompactProtocol.TCompactProtocol(transport)
client = AccumuloProxy.Client(protocol)
transport.open()

userpass = UserPass("root","secret")

print client.tableOperations_list(userpass)

testtable = "pythontest"
if not client.tableOperations_exists(userpass,testtable):
    client.tableOperations_create(userpass,testtable)

row1 = {'a':[PColumnUpdate('a','a',value='value1'), PColumnUpdate('b','b',value='value2')]}
client.updateAndFlush(userpass,testtable,row1,None)

cookie = client.createBatchScanner(userpass,testtable,"",None,None)
print client.scanner_next_k(cookie,10)
