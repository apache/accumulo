
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

require 'rubygems'
require 'thrift'
require 'accumulo_proxy'

server = ARGV[0] || 'localhost'

socket = Thrift::Socket.new(server, 42424, 9001)
transport = Thrift::FramedTransport.new(socket)
proto = Thrift::CompactProtocol.new(transport)
proxy = AccumuloProxy::Client.new(proto)
us = UserPass.new({'username'=> 'root', 'password' => 'secret'})

# open up the connect
transport.open()

# Test if the server is up
login = proxy.login(us)

# print out a table list
puts "List of tables: #{proxy.listTables(us).inspect}"

testtable = "rubytest"
proxy.createTable(us, testtable, true, TimeType::MILLIS) unless proxy.tableExists(us,testtable) 

update1 = ColumnUpdate.new({'colFamily' => "cf1", 'colQualifier' => "cq1", 'value'=> "a"})
update2 = ColumnUpdate.new({'colFamily' => "cf2", 'colQualifier' => "cq2", 'value'=> "b"})
proxy.updateAndFlush(us,testtable,{'row1' => [update1,update2]})

cookie = proxy.createScanner(us,testtable,nil)
result = proxy.nextK(cookie,10)
result.results.each{ |keyvalue| puts "Key: #{keyvalue.key.inspect} Value: #{keyvalue.value}" }

transport.close()
