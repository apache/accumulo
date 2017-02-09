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
package org.apache.accumulo.core.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.thrift.TResourceRequest;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines a request for resources.
 *
 */
public class ResourceRequest implements WritableComparable<ResourceRequest>, Cloneable {

  /**
   * Metadata requests.
   */
  public static ResourceRequest METADATA = new ResourceRequest("METADATA", false);
  protected String queueName;
  protected boolean interruptible;
  protected long memoryRequested = Long.MAX_VALUE;
  protected long cpuTime = Long.MAX_VALUE;
  
  public ResourceRequest(String queueName, boolean interruptible, long memoryRequested, long cpuTime) {
    this.queueName = queueName;
    this.interruptible = interruptible;
    this.memoryRequested = memoryRequested;
    this.cpuTime=cpuTime;
  }
  
  public ResourceRequest(String queueName, boolean interruptible) {
	    this(queueName,interruptible,Long.MAX_VALUE, Long.MAX_VALUE);
	  }

  public ResourceRequest() {}

  /**
   * Converts this key to Thrift.
   *
   * @return Thrift key
   */
  public TResourceRequest toThrift() {
    return new TResourceRequest(queueName, interruptible,memoryRequested,cpuTime);
  }

  public static ResourceRequest fromThrift(TResourceRequest thriftRequest) {
    return new ResourceRequest(thriftRequest.queueName, thriftRequest.interruptible, thriftRequest.mem, thriftRequest.cpuTime);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(queueName);
    out.writeBoolean(interruptible);
    out.writeLong(memoryRequested);
    out.writeLong(cpuTime);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    queueName = in.readUTF();
    interruptible = in.readBoolean();
    memoryRequested = in.readLong();
    cpuTime = in.readLong();
  }

  @Override
  public int compareTo(ResourceRequest o) {
    return queueName.compareTo(o.queueName);
  }

  @Override
  public int hashCode() {
	return new HashCodeBuilder().append(queueName).append(memoryRequested).append(cpuTime).append(interruptible).hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ResourceRequest) {
      return compareTo((ResourceRequest) obj) == 0;
    } else
      return false;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    ResourceRequest r = (ResourceRequest) super.clone();
    r.queueName = queueName;
    r.interruptible = interruptible;
    r.memoryRequested = memoryRequested;
    r.cpuTime = cpuTime;
    return r;
  }
}
