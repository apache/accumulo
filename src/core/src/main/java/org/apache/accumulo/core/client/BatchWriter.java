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
package org.apache.accumulo.core.client;

import org.apache.accumulo.core.data.Mutation;

/**
 * Send Mutations to a single Table in Accumulo.
 */
public interface BatchWriter {
  
  /**
   * Queues one mutation to write.
   * 
   * @param m
   *          the mutation to add
   * @throws MutationsRejectedException
   *           this could be thrown because current or previous mutations failed
   */
  
  public void addMutation(Mutation m) throws MutationsRejectedException;
  
  /**
   * Queues several mutations to write.
   * 
   * @param iterable
   *          allows adding any number of mutations iteratively
   * @throws MutationsRejectedException
   *           this could be thrown because current or previous mutations failed
   */
  public void addMutations(Iterable<Mutation> iterable) throws MutationsRejectedException;
  
  /**
   * Send any buffered mutations to Accumulo immediately.
   * 
   * @throws MutationsRejectedException
   *           this could be thrown because current or previous mutations failed
   */
  public void flush() throws MutationsRejectedException;
  
  /**
   * Flush and release any resources.
   * 
   * @throws MutationsRejectedException
   *           this could be thrown because current or previous mutations failed
   */
  public void close() throws MutationsRejectedException;
  
}
