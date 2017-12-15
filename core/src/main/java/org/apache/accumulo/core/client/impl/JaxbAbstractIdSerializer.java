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
package org.apache.accumulo.core.client.impl;

import javax.xml.bind.annotation.adapters.XmlAdapter;

/**
 * A class for marshaling @link{AbstractId} so REST calls can serialize AbstractId to its canonical value.
 */
public class JaxbAbstractIdSerializer extends XmlAdapter<String,AbstractId> {

  @Override
  public String marshal(AbstractId id) {
    if (id != null)
      return id.canonicalID();
    else
      return null;
  }

  @Override
  public AbstractId unmarshal(String id) {
    // should not unmarshal from String
    throw new UnsupportedOperationException("Cannot unmarshal from String");
  }
}
