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
package org.apache.accumulo.core.client.lexicoder;

import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

/**
 * 
 * @since 1.6.0
 */

public class ReverseLexicoder<T> implements Lexicoder<T> {
  
  private Lexicoder<T> lexicoder;
  
  public ReverseLexicoder(Lexicoder<T> lexicoder) {
    this.lexicoder = lexicoder;
  }
  
  @Override
  public byte[] encode(T data) {
    byte[] bytes = escape(lexicoder.encode(data));
    byte[] ret = new byte[bytes.length + 1];
    
    for (int i = 0; i < bytes.length; i++)
      ret[i] = (byte) (0xff - (0xff & bytes[i]));
    
    ret[bytes.length] = (byte) 0xff;
    
    return ret;
  }
  
  @Override
  public T decode(byte[] data) {
    byte ret[] = new byte[data.length - 1];
    
    for (int i = 0; i < ret.length; i++)
      ret[i] = (byte) (0xff - (0xff & data[i]));
    
    return lexicoder.decode(unescape(ret));
  }
}
