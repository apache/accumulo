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
package org.apache.accumulo.master.util;

import static org.apache.accumulo.core.clientImpl.Tables.VALID_NAME_REGEX;
import static org.apache.accumulo.core.clientImpl.Tables.qualify;

import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.replication.ReplicationTable;
import org.apache.accumulo.core.util.Validator;

import com.google.common.base.Joiner;

public class TableValidators {
  public static final String VALID_ID_REGEX = "^([a-z0-9]+)$"; // BigDecimal base36

  public static final Validator<String> VALID_NAME = new Validator<>() {
    @Override
    public boolean test(String tableName) {
      return tableName != null && tableName.matches(VALID_NAME_REGEX);
    }

    @Override
    public String invalidMessage(String tableName) {
      if (tableName == null)
        return "Table name cannot be null";
      return "Table names must only contain word characters (letters, digits, and underscores): "
          + tableName;
    }
  };

  public static final Validator<TableId> VALID_ID = new Validator<>() {
    @Override
    public boolean test(TableId tableId) {
      return tableId != null && (RootTable.ID.equals(tableId) || MetadataTable.ID.equals(tableId)
          || ReplicationTable.ID.equals(tableId) || tableId.canonical().matches(VALID_ID_REGEX));
    }

    @Override
    public String invalidMessage(TableId tableId) {
      if (tableId == null)
        return "Table id cannot be null";
      return "Table IDs are base-36 numbers, represented with lowercase alphanumeric digits: "
          + tableId;
    }
  };

  public static final Validator<String> NOT_METADATA = new Validator<>() {

    private List<String> metadataTables = Arrays.asList(RootTable.NAME, MetadataTable.NAME);

    @Override
    public boolean test(String tableName) {
      return !metadataTables.contains(tableName);
    }

    @Override
    public String invalidMessage(String tableName) {
      return "Table cannot be one of {" + Joiner.on(",").join(metadataTables) + "}";
    }
  };

  public static final Validator<TableId> CAN_CLONE = new Validator<>() {

    private List<TableId> metaIDs = Arrays.asList(RootTable.ID, MetadataTable.ID);

    @Override
    public boolean test(TableId tableId) {
      return !metaIDs.contains(tableId);
    }

    @Override
    public String invalidMessage(TableId tableId) {
      String msg;
      if (tableId.equals(MetadataTable.ID)) {
        msg = " Cloning " + MetadataTable.NAME
            + " is dangerous and no longer supported, see https://github.com/apache/accumulo/issues/1309.";
      } else {
        msg = "Can not clone " + RootTable.NAME;
      }
      return msg;
    }
  };

  public static final Validator<String> NOT_SYSTEM = new Validator<>() {

    @Override
    public boolean test(String tableName) {
      return !Namespace.ACCUMULO.name().equals(qualify(tableName).getFirst());
    }

    @Override
    public String invalidMessage(String tableName) {
      return "Table cannot be in the " + Namespace.ACCUMULO.name() + " namespace";
    }
  };

  public static final Validator<TableId> NOT_ROOT_ID = new Validator<>() {

    @Override
    public boolean test(TableId tableId) {
      return !RootTable.ID.equals(tableId);
    }

    @Override
    public String invalidMessage(TableId tableId) {
      return "Table cannot be the " + RootTable.NAME + "(Id: " + RootTable.ID + ") table";
    }
  };

}
