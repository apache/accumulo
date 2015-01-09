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

import org.apache.accumulo.core.master.state.tables.TableState;

public class TableStateType extends CellType<TableState> {

  @Override
  public String alignment() {
    return "center";
  }

  @Override
  public String format(Object obj) {
    TableState state = obj == null ? TableState.UNKNOWN : (TableState) obj;
    String style = null;
    switch (state) {
      case ONLINE:
      case OFFLINE:
        break;
      case NEW:
      case DELETING:
        style = "warning";
        break;
      case UNKNOWN:
      default:
        style = "error";
    }
    style = style != null ? " class='" + style + "'" : "";
    return String.format("<span%s>%s</span>", style, state);
  }

  @Override
  public int compare(TableState o1, TableState o2) {
    if (o1 == null && o2 == null)
      return 0;
    else if (o1 == null)
      return -1;
    return o1.compareTo(o2);
  }

}
