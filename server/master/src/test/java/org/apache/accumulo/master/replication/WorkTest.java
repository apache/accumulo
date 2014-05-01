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
package org.apache.accumulo.master.replication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.replication.StatusUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class WorkTest {

  @Test
  public void serialization() throws IOException {
    Work w = new Work("/foo/bar", StatusUtil.openWithUnknownLength());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    w.write(new DataOutputStream(baos));

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

    Work newW = new Work();
    newW.readFields(new DataInputStream(bais));

    Assert.assertEquals(w, newW);
  }

}
