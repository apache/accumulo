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
package org.apache.accumulo.core.tasks;

import java.io.IOException;

import org.apache.accumulo.core.tasks.thrift.TaskObject;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper;

public class TaskDeSer {

  // thread-safe if configured before any read/write calls
  private static final CBORMapper mapper = new CBORMapper();

  static {
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT, true);
    mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
  }

  public static <T extends Task> TaskObject serialize(T task) throws JsonProcessingException {
    TaskObject to = new TaskObject();
    to.setTaskID(task.getTaskId());
    to.setObjectType(task.getClass().getName());
    to.setCborEncodedObject(mapper.writeValueAsBytes(task));
    return to;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Task> T deserialize(TaskObject to)
      throws ClassNotFoundException, StreamReadException, DatabindException, IOException {
    Class<? extends Task> clazz = (Class<? extends Task>) Class.forName(to.getObjectType());
    return (T) mapper.readValue(to.getCborEncodedObject(), clazz);
  }

}
