/*
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
*/
  String accumuloProp = 
      System.getProperty("user.dir").replace("/bin", "/conf/accumulo-client.properties");
  String name; String zk; String principal; String token; 
  AccumuloClient client = null; Authorizations auth = null;
  
  try {
    // Does the accumulo properties file exists?
    if (Files.exists(Paths.get(accumuloProp))) {
      // Build Accumulo Client
      client = Accumulo.newClient().from(accumuloProp).build();
      
      // Test Accumulo Client
      auth = client.securityOperations().getUserAuthorizations(client.whoami());
      if (auth != null) {
        System.out.println("Accumulo Client was Successfully Built!");
      System.out.println("Use " + "\"client\"" + " to interact with Accumulo \n");
      }
    }
    else
      System.out.println("accumulo-client.properties not found in conf directory \n");
        
  } catch (IllegalArgumentException illegalExcept) {
    System.out.println("Build Error: "
        + "Could not build Accumulo client with accumulo-client.properties \n"); 
      
  } catch (AccumuloException accumuloExcept) {
    System.out.println("Connection Error: "
        + "Ensure instance.name and instance.zookeeper are properly configured \n");
        
  } catch (AccumuloSecurityException secExcept) {
    System.out.println( "Security Error: " 
        + "Ensure auth.principal and auth.token are properly configured \n");
        
  } catch (NullPointerException nullExcept) {
    System.out.println("Null Error: "
        + "Could not build Accumulo client with accumulo-client.properties \n");  
  }
