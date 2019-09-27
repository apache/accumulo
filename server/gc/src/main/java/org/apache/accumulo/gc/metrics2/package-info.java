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
package org.apache.accumulo.gc.metrics2;

/**
 * This package contains classes to publish Accumulo Garbage Collection run cycle statistics to the
 * hadoop metrics2 system. The hadoop metrics2 system publishes to jmx and can be configured, via a
 * configuration file, to publish to other metric collection systems (files,...)
 * <p>
 * Accumulo will search for a file named hadoop-metrics2-accumulo.properties on the Accumulo
 * classpath.
 * <p>
 * A note on naming: The naming for jmx vs the metrics2 systems are slightly different. Hadoop
 * metrics2 records will start with CONTEXT.RECORD (accgc.AccGcCycleMetrics). The value for context
 * is also used by the configuration file for sink configuration.
 * <p>
 * In JMX, the hierarchy is: Hadoop..Accumulo..[jmxName]..[processName]..attributes..[name]
 * <p>
 * For jvm metrics, the hierarchy is Hadoop..Accumulo..JvmMetrics..attributes..[name]
 */
