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
package org.apache.accumulo.core.dataImpl;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.UUID;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.TKeyExtent;
import org.apache.accumulo.core.util.ByteBufferUtil;
import org.apache.hadoop.io.Text;

/**
 * By subclassing KeyExtent and adding a unique value, we allow the ScanServer to use this class in
 * OnlineTablets and other places so that the ScanServer can load multiple Tablet definitions for
 * the same extent for the purposes of scanning them.
 *
 */
public class ScanServerKeyExtent extends KeyExtent {

  private final UUID uuid = UUID.randomUUID();

  public ScanServerKeyExtent(TableId table, Text endRow, Text prevEndRow) {
    super(table, endRow, prevEndRow);
  }

  // This is important as it ensures that the ScanServerKeyExtent objects
  // in OnlineTablets are separate keys
  @Override
  public int compareTo(KeyExtent other) {
    int result = super.compareTo(other);
    if (other instanceof ScanServerKeyExtent) {
      if (result == 0) {
        return this.uuid.compareTo(((ScanServerKeyExtent) other).uuid);
      } else {
        return result;
      }
    } else {
      return result;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    ScanServerKeyExtent other = (ScanServerKeyExtent) obj;
    if (uuid == null) {
      if (other.uuid != null)
        return false;
    } else if (!uuid.equals(other.uuid))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "ScanServerKeyExtent [uuid=" + uuid + ", extent=" + super.toString() + "]";
  }

  public KeyExtent toKeyExent() {
    return new KeyExtent(this.tableId(), this.endRow(), this.prevEndRow());
  }

  /**
   * Create a KeyExtent from its Thrift form.
   *
   * @param tke
   *          the KeyExtent in its Thrift object form
   */
  public static ScanServerKeyExtent fromThrift(TKeyExtent tke) {
    TableId tableId = TableId.of(new String(ByteBufferUtil.toBytes(tke.table), UTF_8));
    Text endRow = tke.endRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.endRow));
    Text prevEndRow =
        tke.prevEndRow == null ? null : new Text(ByteBufferUtil.toBytes(tke.prevEndRow));
    return new ScanServerKeyExtent(tableId, endRow, prevEndRow);
  }

}
