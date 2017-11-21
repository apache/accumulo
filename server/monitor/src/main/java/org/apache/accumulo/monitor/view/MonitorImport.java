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
package org.apache.accumulo.monitor.view;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * POJO used for parsing JSON configuration of {@link org.apache.accumulo.core.conf.Property#MONITOR_HEADER_IMPORTS}
 *
 * Includes a helper method for creating the HTML for the Monitor in {@link #generateHTML()}
 */
public class MonitorImport {
  private static final Logger log = LoggerFactory.getLogger(MonitorImport.class);

  public enum Type {
    css, js
  }

  String url;
  String type;
  String integrity;
  String crossOrigin;

  /**
   * Create the appropriate HTML for the import.
   * @return HTML of the monitor import
   */
  public String generateHTML() throws AccumuloException {
    StringBuilder html = new StringBuilder();
    if (StringUtils.isEmpty(this.url)) {
      throw new AccumuloException("Missing url for MonitorImport type: " + this.type);
    }
    if (isType(Type.css)) {
      html.append("<link rel=\'stylesheet\' ");
      html.append("href=\'").append(this.url).append("\' ");
      appendIntegrityCheck(html);
      html.append("/>");
    } else if (isType(Type.js)) {
      html.append("<script language=\'javascript\' ");
      html.append("src=\'").append(this.url).append("\' ");
      appendIntegrityCheck(html);
      html.append("></script>");
    } else {
      throw new AccumuloException("Unknown MonitorImport type found: " + this.type);
    }
    return html.toString();
  }

  private void appendIntegrityCheck(StringBuilder html) {
    if (!StringUtils.isEmpty(this.crossOrigin) && !StringUtils.isEmpty(this.integrity)) {
      html.append("crossOrigin=\'").append(this.crossOrigin).append("\' ");
      html.append("integrity=\'").append(this.integrity).append("\' ");
    }
  }

  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getType() {
    return type;
  }

  public boolean isType(Type typeToCompare) {
    return Type.valueOf(StringUtils.lowerCase(this.type)).equals(typeToCompare);
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getIntegrity() {
    return integrity;
  }

  public void setIntegrity(String integrity) {
    this.integrity = integrity;
  }

  public String getCrossOrigin() {
    return crossOrigin;
  }

  public void setCrossOrigin(String crossOrigin) {
    this.crossOrigin = crossOrigin;
  }
}
