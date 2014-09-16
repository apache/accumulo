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
package org.apache.accumulo.master.state;

import java.io.IOException;
import java.io.ObjectInputStream;

// ACCUMULO-3132
// Total hack around the serialization of TInfo into zookeeper 
// This class has to keep the same name, and its name is hardcoded in the Fate module
public class TInfo extends org.apache.accumulo.trace.thrift.TInfo {
  private static final long serialVersionUID = -4659975753252858243l;
  
  private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(ois)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }
}
