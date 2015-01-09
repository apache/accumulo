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
package org.apache.accumulo.core.client.admin;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.security.Authorizations;

/**
 * A class that contains information about an ActiveScan
 *
 */
public abstract class ActiveScan {

  /**
   * @return an id that uniquely identifies that scan on the server
   */
  public abstract long getScanid();

  /**
   * @return the address of the client that initiated the scan
   */
  public abstract String getClient();

  /**
   * @return the user that initiated the scan
   */
  public abstract String getUser();

  /**
   * @return the table the scan is running against
   */
  public abstract String getTable();

  /**
   * @return the age of the scan in milliseconds
   */
  public abstract long getAge();

  /**
   * @return milliseconds since last time client read data from the scan
   */
  public abstract long getLastContactTime();

  public abstract ScanType getType();

  public abstract ScanState getState();

  /**
   * @return tablet the scan is running against, if a batch scan may be one of many or null
   */
  public abstract KeyExtent getExtent();

  /**
   * @return columns requested by the scan
   */
  public abstract List<Column> getColumns();

  /**
   * @return server side iterators used by the scan
   */
  public abstract List<String> getSsiList();

  /**
   * @return server side iterator options
   */
  public abstract Map<String,Map<String,String>> getSsio();

  /**
   * @return the authorizations being used for this scan
   * @since 1.5.0
   */
  public abstract Authorizations getAuthorizations();

  /**
   * @return the time this scan has been idle in the tablet server
   * @since 1.5.0
   */
  public abstract long getIdleTime();
}
