/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.client.admin;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.AbstractId;

/**
 * Provides information about an administrative operation running on an Accumulo instance.
 *
 * @since 2.1.0
 */
public interface AccumuloOperation {

  /**
   * @since 2.1.0
   */
  enum Type {
    CREATE_TABLE, BULK_IMPORT, COMPACT, MERGE, DELETE_TABLE
  }

  /**
   * @since 2.1.0
   */
  public static interface Status {
    Date getStartTime();
    String getShortDescription();
    String getLongDescription();
  }

  /**
   * @since 2.1.0
   */
  public static class Id extends AbstractId<Id> {

    private static final long serialVersionUID = 1L;

    private Id(String canonical) {
      super(canonical);
    }

    public static Id of(String canonical) {
      return new Id(canonical);
    }
  }

  /**
   * @return an id that uniquely identifies this operation
   */
  Id getId();

  /**
   * @return what kind operation this is.
   */
  Type getType();

  /**
   * @return a single line description of the goal of this operation
   */
  String getGoal();

  /**
   * @return the user provided arguments that were used to initiate this operation. For example a
   *         create table operation would have a table name argument.
   */
  Map<String,byte[]> getArguments();

  /**
   * @return Previously completed steps Accumulo has taken to carry out this operation
   */
  List<Status> getHistory();

  /**
   * @return Information about what Accumulo is currently working on for this operation.
   */
  Status getCurrentStatus();
}
