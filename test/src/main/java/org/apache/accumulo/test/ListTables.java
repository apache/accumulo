/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.test;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.impl.Table;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.server.cli.ClientOpts;

/**
 * This little program is used by the functional test to get a list of table ids.
 */
public class ListTables {
  public static void main(String[] args) throws Exception {
    ClientOpts opts = new ClientOpts();
    opts.parseArgs(ListTables.class.getName(), args);
    for (Entry<String,Table.ID> table : Tables.getNameToIdMap(opts.getInstance()).entrySet())
      System.out.println(table.getKey() + " => " + table.getValue());
  }
}
