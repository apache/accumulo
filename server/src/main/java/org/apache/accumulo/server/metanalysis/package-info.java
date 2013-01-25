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
 * Provides programs to analyze metadata mutations written to write ahead logs.  
 * 
 * <p>
 * These programs can be used when write ahead logs are archived.   The best way to find
 * which write ahead logs contain metadata mutations is to grep the tablet server logs.  
 * Grep for events where walogs were added to metadata tablets, then take the unique set 
 * of walogs.
 *
 * <p>
 * To use these programs, use IndexMeta to index the metadata mutations in walogs into 
 * Accumulo tables.  Then use FindTable and PrintEvents to analyze those indexes.  
 * FilterMetaiallows filtering walogs down to just metadata events.  This is useful for the
 * case where the walogs need to be exported from the cluster for analysis.
 *
 * @since 1.5
 */
package org.apache.accumulo.server.metanalysis;
