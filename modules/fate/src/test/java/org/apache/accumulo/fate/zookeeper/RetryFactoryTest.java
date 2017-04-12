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
package org.apache.accumulo.fate.zookeeper;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RetryFactoryTest {

  @Test
  public void properArgumentsInRetry() {
    long maxRetries = 10, startWait = 50l, maxWait = 5000l, waitIncrement = 500l;
    RetryFactory factory = new RetryFactory(maxRetries, startWait, waitIncrement, maxWait);
    Retry retry = factory.create();

    Assert.assertEquals(maxRetries, retry.getMaxRetries());
    Assert.assertEquals(startWait, retry.getCurrentWait());
    Assert.assertEquals(maxWait, retry.getMaxWait());
    Assert.assertEquals(waitIncrement, retry.getWaitIncrement());
  }

}
