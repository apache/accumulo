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

import java.util.Date;

import org.apache.accumulo.core.clientImpl.lexicoder.AbstractLexicoder;

/**
 * A lexicoder for date objects. It preserves the native Java sort order for Date.
 *
 * @since 1.6.0
 */
public class DateLexicoder extends AbstractLexicoder<Date> {

  private LongLexicoder longEncoder = new LongLexicoder();

  @Override
  public byte[] encode(Date data) {
    return longEncoder.encode(data.getTime());
  }

  @Override
  public Date decode(byte[] b) {
    // This concrete implementation is provided for binary compatibility, since the corresponding
    // superclass method has type-erased return type Object. See ACCUMULO-3789 and #1285.
    return super.decode(b);
  }

  @Override
  protected Date decodeUnchecked(byte[] data, int offset, int len) {
    return new Date(longEncoder.decodeUnchecked(data, offset, len));
  }

}
