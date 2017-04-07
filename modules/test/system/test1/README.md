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

Command to run from command line

Can run this test with pre-existing splits; use the following command to create the table with
100 pre-existing splits 

> `$ ../../../bin/accumulo 'org.apache.accumulo.test.TestIngest' --createTable \  
-u root -p secret --splits 100 --rows 0`

Could try running verify commands after stopping and restarting accumulo

When write ahead log is implemented can try killing tablet server in middle of ingest

Run 5 parallel ingesters and verify:

> `$ . ingest_test.sh`  
(wait)  
`$ . verify_test.sh`  
(wait)

Overwrite previous ingest:
> `$ . ingest_test_2.sh`  
(wait)  
`$ . verify_test_2.sh`  
(wait)

Delete what was previously ingested:
> `$ . ingest_test_3.sh`  
(wait)

