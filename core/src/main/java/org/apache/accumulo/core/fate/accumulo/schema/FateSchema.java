/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.fate.accumulo.schema;

import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

public class FateSchema {

  public static class TxColumnFamily {
    public static final String STR_NAME = "tx";
    public static final Text NAME = new Text(STR_NAME);

    public static final String STATUS = "status";
    public static final ColumnFQ STATUS_COLUMN = new ColumnFQ(NAME, new Text(STATUS));

    public static final String CREATE_TIME = "ctime";
    public static final ColumnFQ CREATE_TIME_COLUMN = new ColumnFQ(NAME, new Text(CREATE_TIME));
  }

  public static class TxInfoColumnFamily {
    public static final String STR_NAME = "txinfo";
    public static final Text NAME = new Text(STR_NAME);

    public static final String TX_NAME = "txname";
    public static final ColumnFQ TX_NAME_COLUMN = new ColumnFQ(NAME, new Text(TX_NAME));

    public static final String AUTO_CLEAN = "autoclean";
    public static final ColumnFQ AUTO_CLEAN_COLUMN = new ColumnFQ(NAME, new Text(AUTO_CLEAN));

    public static final String EXCEPTION = "exception";
    public static final ColumnFQ EXCEPTION_COLUMN = new ColumnFQ(NAME, new Text(EXCEPTION));

    public static final String RETURN_VALUE = "retval";
    public static final ColumnFQ RETURN_VALUE_COLUMN = new ColumnFQ(NAME, new Text(RETURN_VALUE));
  }

  public static class RepoColumnFamily {
    public static final String STR_NAME = "repos";
    public static final Text NAME = new Text(STR_NAME);
  }

}
