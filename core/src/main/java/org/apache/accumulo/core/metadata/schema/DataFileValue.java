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
package org.apache.accumulo.core.metadata.schema;

public class DataFileValue {
  private long size;
  private long numEntries;
  private long time = -1;

  public DataFileValue(long size, long numEntries, long time) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = time;
  }

  public DataFileValue(long size, long numEntries) {
    this.size = size;
    this.numEntries = numEntries;
    this.time = -1;
  }

  public DataFileValue(byte[] encodedDFV) {
    String[] ba = new String(encodedDFV).split(",");

    size = Long.parseLong(ba[0]);
    numEntries = Long.parseLong(ba[1]);

    if (ba.length == 3)
      time = Long.parseLong(ba[2]);
    else
      time = -1;
  }

  public long getSize() {
    return size;
  }

  public long getNumEntries() {
    return numEntries;
  }

  public boolean isTimeSet() {
    return time >= 0;
  }

  public long getTime() {
    return time;
  }

  public byte[] encode() {
    if (time >= 0)
      return ("" + size + "," + numEntries + "," + time).getBytes();
    return ("" + size + "," + numEntries).getBytes();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DataFileValue) {
      DataFileValue odfv = (DataFileValue) o;

      return size == odfv.size && numEntries == odfv.numEntries;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(size + numEntries).hashCode();
  }

  @Override
  public String toString() {
    return size + " " + numEntries;
  }

  public void setTime(long time) {
    if (time < 0)
      throw new IllegalArgumentException();
    this.time = time;
  }
}
