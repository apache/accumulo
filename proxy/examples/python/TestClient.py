#! /usr/bin/env python

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

import sys

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TCompactProtocol

from accumulo import AccumuloProxy
from accumulo.ttypes import *

transport = TSocket.TSocket('localhost', 42424)
transport = TTransport.TFramedTransport(transport)
protocol = TCompactProtocol.TCompactProtocol(transport)
client = AccumuloProxy.Client(protocol)
transport.open()

login = client.login('root', {'password':'secret'})

print client.listTables(login)

testtable = "pythontest"
if not client.tableExists(login, testtable):
    client.createTable(login, testtable, True, TimeType.MILLIS)

row1 = {'a':[ColumnUpdate('a','a',value='value1'), ColumnUpdate('b','b',value='value2')]}
client.updateAndFlush(login, testtable, row1)

cookie = client.createScanner(login, testtable, None)
for entry in client.nextK(cookie, 10).results:
   print entry
