/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.monitor.rest;

import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlRootElement;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@XmlRootElement(name = "conf")
public class ConfigInformation {
  private static final Logger log = LoggerFactory.getLogger(ConfigInformation.class);

  public List<String> property = new ArrayList<>();

  // required
  public ConfigInformation() {}

  public ConfigInformation(AccumuloConfiguration config) {
    log.info("Populating config info");
    for (var entry : config) {
      if (!Property.isSensitive(entry.getKey()))
        property.add(entry.getKey() + "=" + entry.getValue());
    }
  }
}
