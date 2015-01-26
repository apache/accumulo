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
package org.apache.accumulo.core.util;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * Utility to help manage binary compatibility between Hadoop versions 1 and 2.
 */
public class HadoopCompatUtil {

  /**
   * Uses reflection to pull Configuration out of the JobContext for Hadoop 1 and Hadoop2 compatibility
   * @param context
   *          The job context for which to return the configuration
   * @return
   *          The Hadoop Configuration- irrespective of the version of Hadoop on the classpath.
   */
  public static Configuration getConfiguration(JobContext context) {
    try {
      Class<?> c = HadoopCompatUtil.class.getClassLoader().loadClass("org.apache.hadoop.mapreduce.JobContext");
      Method m = c.getMethod("getConfiguration");
      Object o = m.invoke(context, new Object[0]);
      return (Configuration) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
