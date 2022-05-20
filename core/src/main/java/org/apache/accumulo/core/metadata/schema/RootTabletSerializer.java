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
package org.apache.accumulo.core.metadata.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.ServerColumnFamily;
import org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.TabletColumnFamily;

/**
 * Serializes the root tablet metadata as Json.
 */
public class RootTabletSerializer {
  /**
   * This class is only static helper methods.
   */
  private RootTabletSerializer() {}

  /**
   * Assumes the byte Array is UTF8 encoded.
   */
  public static RootTabletJson fromJson(byte[] bs) {
    return new RootTabletJson(new String(bs, UTF_8));
  }

  /**
   * Generate initial json for the root tablet metadata. Return the JSON converted to a byte[].
   */
  public static byte[] getInitialJson(String dirName, String file) {
    ServerColumnFamily.validateDirCol(dirName);
    Mutation mutation = TabletColumnFamily.createPrevRowMutation(RootTable.EXTENT);
    ServerColumnFamily.DIRECTORY_COLUMN.put(mutation, new Value(dirName));

    mutation.put(DataFileColumnFamily.STR_NAME, file, new DataFileValue(0, 0).encodeAsValue());

    ServerColumnFamily.TIME_COLUMN.put(mutation,
        new Value(new MetadataTime(0, TimeType.LOGICAL).encode()));

    RootTabletJson rootTabletJson = new RootTabletJson();
    rootTabletJson.update(mutation);

    return rootTabletJson.toJson().getBytes(UTF_8);
  }
}
