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
package org.apache.accumulo.manager.tableOps.bulkVer2;

import static org.apache.accumulo.core.client.ConditionalWriter.Status.ACCEPTED;
import static org.apache.accumulo.core.client.ConditionalWriter.Status.REJECTED;
import static org.apache.accumulo.core.metadata.schema.MetadataSchema.BulkExtDirSection.ID_COL;

import java.util.regex.Pattern;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.clientImpl.AcceptableThriftTableOperationException;
import org.apache.accumulo.core.clientImpl.thrift.TableOperation;
import org.apache.accumulo.core.clientImpl.thrift.TableOperationExceptionType;
import org.apache.accumulo.core.clientImpl.thrift.ThriftTableOperationException;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.fate.FateId;
import org.apache.accumulo.core.fate.Repo;
import org.apache.accumulo.core.metadata.SystemTables;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.manager.tableOps.FateEnv;
import org.apache.accumulo.server.util.bulkCommand.ListBulk;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ReserveExternalDir extends AbstractBulkFateOperation {
  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(ComputeBulkRange.class);

  private static final Pattern BULK_EXTERNAL_PATTERN =
      Pattern.compile(Constants.BULK_EXT_PREFIX + "[0-9a-z]{13}");

  public ReserveExternalDir(TableId tableId, String sourceDir, boolean setTime)
      throws ThriftTableOperationException {
    super(BulkInfo.create(tableId, sourceDir, setTime, true));
    Preconditions.checkArgument(!SystemTables.containsTableId(tableId)); // TODO is this check done
                                                                         // elsewhere?
    if (!BULK_EXTERNAL_PATTERN.matcher(new Path(sourceDir).getName()).matches()) {
      throw new ThriftTableOperationException(tableId.canonical(), null, TableOperation.IMPORT,
          TableOperationExceptionType.OTHER,
          "Unacceptable bulk directory name. Ensure directory names are allocated using the Accumulo API.");
    }

  }

  private boolean reserveDirectoryName(FateId fateId, FateEnv environment) throws Exception {

    var bulkDirName = new Path(bulkInfo.sourceDir).getName();
    var row = MetadataSchema.BulkExtDirSection.prefix(bulkDirName);

    ConditionalMutation cm = new ConditionalMutation(row);
    // add a condition requiring the column does not exist
    cm.addCondition(new Condition(ID_COL.getColumnFamily(), ID_COL.getColumnQualifier()));
    cm.at().family(ID_COL.getColumnFamily()).qualifier(ID_COL.getColumnQualifier())
        .put(fateId.canonical());
    var result = environment.getContext().getSharedUserWriter().get().write(cm);
    if (result.getStatus() == ACCEPTED) {
      return true;
    }

    try (var scanner = environment.getContext().createScanner(SystemTables.METADATA.tableName(),
        Authorizations.EMPTY)) {
      scanner.setRange(Range.exact(row));
      ID_COL.fetch(scanner);
      var iterator = scanner.iterator();
      if (iterator.hasNext()) {
        var entry = iterator.next();
        // When this code runs a second time, in the case of failure, the column will exist. If our
        // fate id is set on the column, then we reserved it.
        return entry.getValue().toString().equals(fateId.canonical());
      } else {
        return reserveDirectoryName(fateId, environment);
      }
    }
  }

  @Override
  public Repo<FateEnv> call(FateId fateId, FateEnv environment) throws Exception {
    if (reserveDirectoryName(fateId, environment)) {
      return new ComputeBulkRange(bulkInfo);
    } else {
      throw new AcceptableThriftTableOperationException(bulkInfo.tableId.canonical(), null,
          TableOperation.IMPORT, TableOperationExceptionType.OTHER,
          "Bulk directory name was used by a previous bulk import operation.");
    }
  }

  @Override
  public void undo(FateId fateId, FateEnv environment) throws Exception {
    var bulkDirName = new Path(bulkInfo.sourceDir).getName();
    var row = MetadataSchema.BulkExtDirSection.prefix(bulkDirName);

    ConditionalMutation cm = new ConditionalMutation(row);
    cm.addCondition(new Condition(ID_COL.getColumnFamily(), ID_COL.getColumnQualifier())
        .setValue(fateId.canonical()));
    cm.at().family(ID_COL.getColumnFamily()).qualifier(ID_COL.getColumnQualifier()).delete();
    var result = environment.getContext().getSharedUserWriter().get().write(cm);

    // The rejected case can occur for two reason, first when this code runs a second time for the
    // same fate id, second when another fate id held the reservation.
    if (result.getStatus() != ACCEPTED && result.getStatus() != REJECTED) {
      log.warn("Failed to delete bulk dir reservation: {} {} {}", fateId, bulkDirName,
          result.getStatus());
    }
  }

  @Override
  public ListBulk.BulkState getState() {
    return ListBulk.BulkState.PREPARING;
  }

}
