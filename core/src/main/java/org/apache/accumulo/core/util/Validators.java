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
package org.apache.accumulo.core.util;

import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.util.tables.TableNameUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

public class Validators {
  private static final Logger log = LoggerFactory.getLogger(Validators.class);

  // do not instantiate
  private Validators() {}

  private static final int MAX_SEGMENT_LEN = 1024;
  private static final Pattern SEGMENT_PATTERN = Pattern.compile("\\w{1," + MAX_SEGMENT_LEN + "}");
  // before we added the length restriction; some existing tables may still be long
  private static final Pattern EXISTING_SEGMENT_PATTERN = Pattern.compile("\\w+");

  private enum NameSegment {
    Table, Namespace;

    Optional<String> isNull() {
      return Optional.of(name() + " name must not be null");
    }

    Optional<String> isBlank() {
      return Optional.of(name() + " name must not be blank");
    }

    Optional<String> tooLong(String s) {
      return Optional
          .of(name() + " name exceeds a maximum length of " + MAX_SEGMENT_LEN + ": " + s);
    }

    Optional<String> invalidChars(String s) {
      return Optional.of(name() + " name '" + s + "' contains invalid (non-word) characters.");
    }

    void warnTooLong(String s) {
      log.warn(name() + " name exceeds a length of {};"
          + " Excessively long names are not supported and can result in unexpected behavior."
          + " Please rename {}", MAX_SEGMENT_LEN, s);
    }

  }

  // namespace name validators

  // common implementation for EXISTING_NAMESPACE_NAME and NEW_NAMESPACE_NAME
  private static Optional<String> _namespaceName(String ns, boolean existing) {
    if (ns == null) {
      return NameSegment.Namespace.isNull();
    }
    // special case for default namespace, which always exists
    if (ns.isEmpty()) {
      return Validator.OK;
    }
    if (ns.length() > MAX_SEGMENT_LEN) {
      if (existing) {
        NameSegment.Namespace.warnTooLong(ns);
      } else {
        return NameSegment.Namespace.tooLong(ns);
      }
    }
    if ((existing ? EXISTING_SEGMENT_PATTERN : SEGMENT_PATTERN).matcher(ns).matches()) {
      return Validator.OK;
    }
    return NameSegment.Namespace.invalidChars(ns);
  }

  public static final Validator<String> EXISTING_NAMESPACE_NAME =
      new Validator<>(ns -> _namespaceName(ns, true));

  public static final Validator<String> NEW_NAMESPACE_NAME =
      new Validator<>(ns -> _namespaceName(ns, false));

  public static final Validator<String> NOT_BUILTIN_NAMESPACE = new Validator<>(ns -> {
    if (ns == null) {
      return NameSegment.Namespace.isNull();
    }
    if (Namespace.DEFAULT.name().equals(ns)) {
      return Optional.of("Namespace must not be the reserved empty namespace");
    }
    if (Namespace.ACCUMULO.name().equals(ns)) {
      return Optional.of("Namespace must not be the reserved namespace, " + ns);
    }
    return Validator.OK;
  });

  // table name validators

  // common implementation for EXISTING_TABLE_NAME and NEW_TABLE_NAME
  private static Optional<String> _tableName(String tableName, boolean existing) {
    if (tableName == null) {
      return NameSegment.Table.isNull();
    }
    int dotPosition = tableName.indexOf('.');
    if (dotPosition == 0) {
      return Optional.of("Table name must include a namespace prior to a dot(.) character");
    }
    String tablePart = tableName;
    if (dotPosition > 0) {
      String namespacePart = tableName.substring(0, dotPosition);
      if (!EXISTING_SEGMENT_PATTERN.matcher(namespacePart).matches()) {
        return NameSegment.Namespace.invalidChars(namespacePart);
      }
      tablePart = tableName.substring(dotPosition + 1);
    }
    if (tablePart.isBlank()) {
      return NameSegment.Table.isBlank();
    }
    if (tablePart.length() > MAX_SEGMENT_LEN) {
      if (existing) {
        NameSegment.Table.warnTooLong(tablePart);
      } else {
        return NameSegment.Table.tooLong(tablePart);
      }
    }
    if (!(existing ? EXISTING_SEGMENT_PATTERN : SEGMENT_PATTERN).matcher(tablePart).matches()) {
      return NameSegment.Table.invalidChars(tablePart);
    }
    return Validator.OK;
  }

  public static final Validator<String> EXISTING_TABLE_NAME =
      new Validator<>(tableName -> _tableName(tableName, true));

  public static final Validator<String> NEW_TABLE_NAME =
      new Validator<>(tableName -> _tableName(tableName, false));

  private static final List<String> metadataTables = List.of(RootTable.NAME, MetadataTable.NAME);
  public static final Validator<String> NOT_METADATA_TABLE = new Validator<>(t -> {
    if (t == null) {
      return NameSegment.Table.isNull();
    }
    if (metadataTables.contains(t)) {
      return Optional.of("Table must not be any of {" + Joiner.on(",").join(metadataTables) + "}");
    }
    return Validator.OK;
  });

  public static final Validator<String> NOT_BUILTIN_TABLE = new Validator<>(t -> {
    if (Namespace.ACCUMULO.name().equals(TableNameUtil.qualify(t).getFirst())) {
      return Optional.of("Table must not be in the '" + Namespace.ACCUMULO.name() + "' namespace");
    }
    return Validator.OK;
  });

  public static Validator<String> sameNamespaceAs(String oldTableName) {
    final String oldNamespace = TableNameUtil.qualify(oldTableName).getFirst();
    return new Validator<>(newName -> {
      if (!oldNamespace.equals(TableNameUtil.qualify(newName).getFirst())) {
        return Optional
            .of("Unable to move tables to a new namespace by renaming. The namespace for " + newName
                + " does not match " + oldTableName);
      }
      return Validator.OK;
    });
  }

  // table id validators

  private static final Pattern VALID_ID_PATTERN = Pattern.compile("[a-z0-9]+"); // BigDecimal base36
  public static final Validator<TableId> VALID_TABLE_ID = new Validator<>(id -> {
    if (id == null) {
      return Optional.of("Table id must not be null");
    }
    if (RootTable.ID.equals(id) || MetadataTable.ID.equals(id)
        || VALID_ID_PATTERN.matcher(id.canonical()).matches()) {
      return Validator.OK;
    }
    return Optional
        .of("Table IDs are base-36 numbers, represented with lowercase alphanumeric digits: " + id);
  });

  public static final Validator<TableId> CAN_CLONE_TABLE = new Validator<>(id -> {
    if (id == null) {
      return Optional.of("Table id must not be null");
    }
    if (id.equals(MetadataTable.ID)) {
      return Optional.of("Cloning " + MetadataTable.NAME + " is dangerous and no longer supported,"
          + " see https://github.com/apache/accumulo/issues/1309.");
    }
    if (id.equals(RootTable.ID)) {
      return Optional.of("Unable to clone " + RootTable.NAME);
    }
    return Validator.OK;
  });

  public static final Validator<TableId> NOT_ROOT_TABLE_ID = new Validator<>(id -> {
    if (id == null) {
      return Optional.of("Table id must not be null");
    }
    if (RootTable.ID.equals(id)) {
      return Optional
          .of("Table must not be the " + RootTable.NAME + "(Id: " + RootTable.ID + ") table");
    }
    return Validator.OK;
  });

}
