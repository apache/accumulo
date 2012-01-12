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
package org.apache.accumulo.examples.wikisearch.sample;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

@XmlAccessorType(XmlAccessType.FIELD)
public class Document {
  
  @XmlElement
  private String id = null;
  
  @XmlElement
  private List<Field> field = new ArrayList<Field>();
  
  public Document() {
    super();
  }
  
  public Document(String id, List<Field> fields) {
    super();
    this.id = id;
    this.field = fields;
  }
  
  public String getId() {
    return id;
  }
  
  public List<Field> getFields() {
    return field;
  }
  
  public void setId(String id) {
    this.id = id;
  }
  
  public void setFields(List<Field> fields) {
    this.field = fields;
  }
  
}
