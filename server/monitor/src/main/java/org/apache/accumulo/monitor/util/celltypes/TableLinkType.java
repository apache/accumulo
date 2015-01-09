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
package org.apache.accumulo.monitor.util.celltypes;

import java.util.Map;

import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.monitor.servlets.BasicServlet;
import org.apache.accumulo.server.client.HdfsZooInstance;

public class TableLinkType extends CellType<String> {

  private Map<String,String> tidToNameMap;

  public TableLinkType() {
    tidToNameMap = Tables.getIdToNameMap(HdfsZooInstance.getInstance());
  }

  @Override
  public String format(Object obj) {
    if (obj == null)
      return "-";
    String tableId = (String) obj;
    // Encode the tableid we use in the link so we construct a correct url
    // e.g. the root table's id of "+r" would not be interpreted properly
    return String.format("<a href='/tables?t=%s'>%s</a>", BasicServlet.encode(tableId), displayName(tableId));
  }

  private String displayName(String tableId) {
    if (tableId == null)
      return "-";
    return Tables.getPrintableTableNameFromId(tidToNameMap, tableId);
  }

  @Override
  public int compare(String o1, String o2) {
    return displayName(o1).compareTo(displayName(o2));
  }

  @Override
  public String alignment() {
    return "left";
  }

}
