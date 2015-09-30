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
package org.apache.accumulo.core.file;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public interface FileSKVWriter extends AutoCloseable {
  boolean supportsLocalityGroups();

  void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException;

  void startDefaultLocalityGroup() throws IOException;

  void append(Key key, Value value) throws IOException;

  DataOutputStream createMetaStore(String name) throws IOException;

  @Override
  void close() throws IOException;

  long getLength() throws IOException;
}
