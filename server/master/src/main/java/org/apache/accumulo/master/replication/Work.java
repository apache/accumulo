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
package org.apache.accumulo.master.replication;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.replication.proto.Replication.Status;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.protobuf.TextFormat;

/**
 * Encapsulates a file (path) and {@link Status}
 */
public class Work implements Writable {

  private String file;

  private Status status;

  public Work() { }

  public Work(String file, Status status) {
    this.file = file;
    this.status = status;
  }

  public String getFile() {
    return file;
  }

  public void setFile(String file) {
    this.file = file;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, file);
    byte[] bytes = status.toByteArray();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    file = WritableUtils.readString(in);
    int len = WritableUtils.readVInt(in);
    byte[] bytes = new byte[len];
    in.readFully(bytes);
    status = Status.parseFrom(bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof Work) {
      Work other = (Work) o;
      return file.equals(other.getFile()) && status.equals(other.getStatus());
    }

    return false;
  }
 
  @Override
  public String toString() {
    return file + " " + TextFormat.shortDebugString(status);
  }

  @Override
  public int hashCode() {
    return file.hashCode() ^ status.hashCode();
  }

}
