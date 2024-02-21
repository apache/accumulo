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
package org.apache.accumulo.core.metadata.schema;

import org.apache.accumulo.core.data.AbstractId;
import org.apache.accumulo.core.fate.FateId;

import com.google.common.base.Preconditions;

/**
 * Intended to contain a globally unique id that identifies an operation running against a tablet.
 * The purpose of this is to prevent race conditions.
 */
public class TabletOperationId extends AbstractId<TabletOperationId> {

  private static final long serialVersionUID = 1L;

  public static String validate(String opid) {
    var fields = opid.split(":", 2);
    Preconditions.checkArgument(fields.length == 2, "Malformed operation id %s", opid);
    try {
      TabletOperationType.valueOf(fields[0]);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Malformed operation id " + opid, e);
    }

    if (!FateId.isFateId(fields[1])) {
      throw new IllegalArgumentException("Malformed operation id " + opid);
    }

    return opid;
  }

  private TabletOperationId(String canonical) {
    super(canonical);
  }

  public TabletOperationType getType() {
    var fields = canonical().split(":", 2);
    Preconditions.checkState(fields.length == 2);
    return TabletOperationType.valueOf(fields[0]);
  }

  public static TabletOperationId from(String opid) {
    return new TabletOperationId(validate(opid));
  }

  public static TabletOperationId from(TabletOperationType type, FateId fateId) {
    return new TabletOperationId(type + ":" + fateId);
  }
}
