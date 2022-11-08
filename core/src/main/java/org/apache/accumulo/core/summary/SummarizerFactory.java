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
package org.apache.accumulo.core.summary;

import java.io.IOException;

import org.apache.accumulo.core.classloader.ClassLoaderUtil;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;

public class SummarizerFactory {
  private ClassLoader classloader;
  private String context;

  public SummarizerFactory() {
    this.classloader = SummarizerFactory.class.getClassLoader();
  }

  public SummarizerFactory(ClassLoader classloader) {
    this.classloader = classloader;
  }

  public SummarizerFactory(AccumuloConfiguration tableConfig) {
    this.context = ClassLoaderUtil.tableContext(tableConfig);
  }

  private Summarizer newSummarizer(String classname)
      throws IOException, ReflectiveOperationException {
    if (classloader != null) {
      return classloader.loadClass(classname).asSubclass(Summarizer.class).getDeclaredConstructor()
          .newInstance();
    } else {
      return ClassLoaderUtil.loadClass(context, classname, Summarizer.class)
          .getDeclaredConstructor().newInstance();
    }
  }

  public Summarizer getSummarizer(SummarizerConfiguration conf) {
    try {
      return newSummarizer(conf.getClassName());
    } catch (ReflectiveOperationException | IOException e) {
      throw new RuntimeException(e);
    }
  }
}
