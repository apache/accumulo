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
package org.apache.accumulo.monitor.rest.trace;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Generates addiotional information for the selected trace
 *
 * @since 2.0.0
 *
 */
public class AddlInformation {

  // Variable names become JSON keys
  public List<DataInformation> data = new ArrayList<>();
  public List<AnnotationInformation> annotations = new ArrayList<>();

  /**
   * Initializes data and annotation array lists
   */
  public AddlInformation() {}

  /**
   * Add a new data
   *
   * @param data
   *          Data to add
   */
  public void addData(DataInformation data) {
    this.data.add(data);
  }

  /**
   * Add a new annotation
   *
   * @param annotations
   *          Annotation to add
   */
  public void addAnnotations(AnnotationInformation annotations) {
    this.annotations.add(annotations);
  }
}
