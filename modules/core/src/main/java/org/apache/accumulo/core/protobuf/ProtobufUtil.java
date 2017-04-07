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
package org.apache.accumulo.core.protobuf;

import org.apache.accumulo.core.data.Value;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.TextFormat;

/**
 * Helper methods for interacting with Protocol Buffers and Accumulo
 */
public class ProtobufUtil {
  private static final char LEFT_BRACKET = '[', RIGHT_BRACKET = ']';

  public static Value toValue(GeneratedMessage msg) {
    return new Value(msg.toByteArray());
  }

  public static String toString(GeneratedMessage msg) {
    // Too much typing
    return LEFT_BRACKET + TextFormat.shortDebugString(msg) + RIGHT_BRACKET;
  }
}
