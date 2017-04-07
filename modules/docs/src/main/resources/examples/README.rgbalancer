Title: Apache Accumulo Hello World Example
Notice:    Licensed to the Apache Software Foundation (ASF) under one
           or more contributor license agreements.  See the NOTICE file
           distributed with this work for additional information
           regarding copyright ownership.  The ASF licenses this file
           to you under the Apache License, Version 2.0 (the
           "License"); you may not use this file except in compliance
           with the License.  You may obtain a copy of the License at
           .
             http://www.apache.org/licenses/LICENSE-2.0
           .
           Unless required by applicable law or agreed to in writing,
           software distributed under the License is distributed on an
           "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
           KIND, either express or implied.  See the License for the
           specific language governing permissions and limitations
           under the License.

For some data access patterns, its important to spread groups of tablets within
a table out evenly.  Accumulo has a balancer that can do this using a regular
expression to group tablets. This example shows how this balancer spreads 4
groups of tablets within a table evenly across 17 tablet servers.

Below shows creating a table and adding splits.  For this example we would like
all of the tablets where the split point has the same two digits to be on
different tservers.  This gives us four groups of tablets: 01, 02, 03, and 04.   

    root@accumulo> createtable testRGB
    root@accumulo testRGB> addsplits -t testRGB 01b 01m 01r 01z  02b 02m 02r 02z 03b 03m 03r 03z 04a 04b 04c 04d 04e 04f 04g 04h 04i 04j 04k 04l 04m 04n 04o 04p
    root@accumulo testRGB> tables -l
    accumulo.metadata    =>        !0
    accumulo.replication =>      +rep
    accumulo.root        =>        +r
    testRGB              =>         2
    trace                =>         1

After adding the splits we look at the locations in the metadata table.

    root@accumulo testRGB> scan -t accumulo.metadata -b 2; -e 2< -c loc
    2;01b loc:34a5f6e086b000c []    ip-10-1-2-25:9997
    2;01m loc:34a5f6e086b000c []    ip-10-1-2-25:9997
    2;01r loc:14a5f6e079d0011 []    ip-10-1-2-15:9997
    2;01z loc:14a5f6e079d000f []    ip-10-1-2-13:9997
    2;02b loc:34a5f6e086b000b []    ip-10-1-2-26:9997
    2;02m loc:14a5f6e079d000c []    ip-10-1-2-28:9997
    2;02r loc:14a5f6e079d0012 []    ip-10-1-2-27:9997
    2;02z loc:14a5f6e079d0012 []    ip-10-1-2-27:9997
    2;03b loc:14a5f6e079d000d []    ip-10-1-2-21:9997
    2;03m loc:14a5f6e079d000e []    ip-10-1-2-20:9997
    2;03r loc:14a5f6e079d000d []    ip-10-1-2-21:9997
    2;03z loc:14a5f6e079d000e []    ip-10-1-2-20:9997
    2;04a loc:34a5f6e086b000b []    ip-10-1-2-26:9997
    2;04b loc:14a5f6e079d0010 []    ip-10-1-2-17:9997
    2;04c loc:14a5f6e079d0010 []    ip-10-1-2-17:9997
    2;04d loc:24a5f6e07d3000c []    ip-10-1-2-16:9997
    2;04e loc:24a5f6e07d3000d []    ip-10-1-2-29:9997
    2;04f loc:24a5f6e07d3000c []    ip-10-1-2-16:9997
    2;04g loc:24a5f6e07d3000a []    ip-10-1-2-14:9997
    2;04h loc:14a5f6e079d000c []    ip-10-1-2-28:9997
    2;04i loc:34a5f6e086b000d []    ip-10-1-2-19:9997
    2;04j loc:34a5f6e086b000d []    ip-10-1-2-19:9997
    2;04k loc:24a5f6e07d30009 []    ip-10-1-2-23:9997
    2;04l loc:24a5f6e07d3000b []    ip-10-1-2-22:9997
    2;04m loc:24a5f6e07d30009 []    ip-10-1-2-23:9997
    2;04n loc:24a5f6e07d3000b []    ip-10-1-2-22:9997
    2;04o loc:34a5f6e086b000a []    ip-10-1-2-18:9997
    2;04p loc:24a5f6e07d30008 []    ip-10-1-2-24:9997
    2< loc:24a5f6e07d30008 []    ip-10-1-2-24:9997

Below the information above was massaged to show which tablet groups are on
each tserver.  The four tablets in group 03 are on two tservers, ideally those
tablets would be spread across 4 tservers.  Note the default tablet (2<) was
categorized as group 04 below.

    ip-10-1-2-13:9997 01
    ip-10-1-2-14:9997 04
    ip-10-1-2-15:9997 01
    ip-10-1-2-16:9997 04 04
    ip-10-1-2-17:9997 04 04
    ip-10-1-2-18:9997 04
    ip-10-1-2-19:9997 04 04
    ip-10-1-2-20:9997 03 03
    ip-10-1-2-21:9997 03 03
    ip-10-1-2-22:9997 04 04
    ip-10-1-2-23:9997 04 04
    ip-10-1-2-24:9997 04 04
    ip-10-1-2-25:9997 01 01
    ip-10-1-2-26:9997 02 04
    ip-10-1-2-27:9997 02 02
    ip-10-1-2-28:9997 02 04
    ip-10-1-2-29:9997 04

To remedy this situation, the RegexGroupBalancer is configured with the
commands below.  The configured regular expression selects the first two digits
from a tablets end row as the group id.  Tablets that don't match and the
default tablet are configured to be in group 04.

    root@accumulo testRGB> config -t testRGB -s table.custom.balancer.group.regex.pattern=(\\d\\d).*
    root@accumulo testRGB> config -t testRGB -s table.custom.balancer.group.regex.default=04
    root@accumulo testRGB> config -t testRGB -s table.balancer=org.apache.accumulo.server.master.balancer.RegexGroupBalancer

After waiting a little bit, look at the tablet locations again and all is good.

    root@accumulo testRGB> scan -t accumulo.metadata -b 2; -e 2< -c loc
    2;01b loc:34a5f6e086b000a []    ip-10-1-2-18:9997
    2;01m loc:34a5f6e086b000c []    ip-10-1-2-25:9997
    2;01r loc:14a5f6e079d0011 []    ip-10-1-2-15:9997
    2;01z loc:14a5f6e079d000f []    ip-10-1-2-13:9997
    2;02b loc:34a5f6e086b000b []    ip-10-1-2-26:9997
    2;02m loc:14a5f6e079d000c []    ip-10-1-2-28:9997
    2;02r loc:34a5f6e086b000d []    ip-10-1-2-19:9997
    2;02z loc:14a5f6e079d0012 []    ip-10-1-2-27:9997
    2;03b loc:24a5f6e07d3000d []    ip-10-1-2-29:9997
    2;03m loc:24a5f6e07d30009 []    ip-10-1-2-23:9997
    2;03r loc:14a5f6e079d000d []    ip-10-1-2-21:9997
    2;03z loc:14a5f6e079d000e []    ip-10-1-2-20:9997
    2;04a loc:34a5f6e086b000b []    ip-10-1-2-26:9997
    2;04b loc:34a5f6e086b000c []    ip-10-1-2-25:9997
    2;04c loc:14a5f6e079d0010 []    ip-10-1-2-17:9997
    2;04d loc:14a5f6e079d000e []    ip-10-1-2-20:9997
    2;04e loc:24a5f6e07d3000d []    ip-10-1-2-29:9997
    2;04f loc:24a5f6e07d3000c []    ip-10-1-2-16:9997
    2;04g loc:24a5f6e07d3000a []    ip-10-1-2-14:9997
    2;04h loc:14a5f6e079d000c []    ip-10-1-2-28:9997
    2;04i loc:14a5f6e079d0011 []    ip-10-1-2-15:9997
    2;04j loc:34a5f6e086b000d []    ip-10-1-2-19:9997
    2;04k loc:14a5f6e079d0012 []    ip-10-1-2-27:9997
    2;04l loc:14a5f6e079d000f []    ip-10-1-2-13:9997
    2;04m loc:24a5f6e07d30009 []    ip-10-1-2-23:9997
    2;04n loc:24a5f6e07d3000b []    ip-10-1-2-22:9997
    2;04o loc:34a5f6e086b000a []    ip-10-1-2-18:9997
    2;04p loc:14a5f6e079d000d []    ip-10-1-2-21:9997
    2< loc:24a5f6e07d30008 []    ip-10-1-2-24:9997

Once again, the data above is transformed to make it easier to see which groups
are on tservers.  The transformed data below shows that all groups are now
evenly spread.

    ip-10-1-2-13:9997 01 04
    ip-10-1-2-14:9997    04
    ip-10-1-2-15:9997 01 04
    ip-10-1-2-16:9997    04
    ip-10-1-2-17:9997    04
    ip-10-1-2-18:9997 01 04
    ip-10-1-2-19:9997 02 04
    ip-10-1-2-20:9997 03 04
    ip-10-1-2-21:9997 03 04
    ip-10-1-2-22:9997    04
    ip-10-1-2-23:9997 03 04
    ip-10-1-2-24:9997    04
    ip-10-1-2-25:9997 01 04
    ip-10-1-2-26:9997 02 04
    ip-10-1-2-27:9997 02 04
    ip-10-1-2-28:9997 02 04
    ip-10-1-2-29:9997 03 04

If you need this functionality, but a regular expression does not meet your
needs then extend GroupBalancer.  This allows you to specify a partitioning
function in Java.  Use the RegexGroupBalancer source as an example.
