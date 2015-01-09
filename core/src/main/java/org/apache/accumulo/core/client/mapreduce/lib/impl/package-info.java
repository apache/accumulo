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
/**
 * This package exists to store common helpers for configuring MapReduce jobs in a single location. It contains static configurator methods, stored in classes
 * separate from the things they configure (typically, {@link org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat}/
 * {@link org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat} and related classes in compatible frameworks), rather than storing them in those
 * InputFormats/OutputFormats, so as not to clutter their API with methods that don't match the conventions for that framework. These classes may be useful to
 * input/output plugins for other frameworks, so they can reuse the same configuration options and/or serialize them into a
 * {@link org.apache.hadoop.conf.Configuration} instance in a standard way.
 *
 * <p>
 * It is not expected these will change much (except when new features are added), but end users should not use these classes. They should use the static
 * configurators on the {@link org.apache.hadoop.mapreduce.InputFormat} or {@link org.apache.hadoop.mapreduce.OutputFormat} they are configuring, which in turn
 * may use these classes to implement their own static configurators. Once again, these classes are intended for internal use, but may be useful to developers
 * of plugins for other frameworks that read/write to Accumulo.
 *
 * @since 1.6.0
 */
package org.apache.accumulo.core.client.mapreduce.lib.impl;

