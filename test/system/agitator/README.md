<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at 
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Agitator: randomly kill processes
===========================

The agitator is used to randomly select processes for termination during
system test.

Configure the agitator using the example agitator.ini file provided.

Create a list of hosts to be agitated:

	$ cp ../../../conf/tservers hosts
	$ echo master >> hosts
	$ echo namenode >> hosts

The agitator can be used to kill and restart any part of the accumulo
ecosystem: zookeepers, namenode, datanodes, tablet servers and master.
You can choose to agitate them all with "--all"

	$ ./agitator.py --all --hosts=hosts --config=agitator.ini --log DEBUG

You will need to be able to ssh, without passwords, to all your hosts as 
the user that can kill and start the services.
