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
package org.apache.accumulo.test;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;

import org.apache.accumulo.core.util.Base64;
import org.junit.Test;

public class TraceRepoDeserializationTest {

  // Zookeeper data for a merge request
  static private final String oldValue = "rO0ABXNyAC1vcmcuYXBhY2hlLmFjY3VtdWxvLm1hc3Rlci50YWJsZU9wcy5UcmFjZVJlc"
      + "G8AAAAAAAAAAQIAAkwABHJlcG90AB9Mb3JnL2FwYWNoZS9hY2N1bXVsby9mYXRlL1Jl" + "cG87TAAFdGluZm90AChMb3JnL2FwYWNoZS9hY2N1bXVsby90cmFjZS90aHJpZnQvVEl"
      + "uZm87eHBzcgAwb3JnLmFwYWNoZS5hY2N1bXVsby5tYXN0ZXIudGFibGVPcHMuVGFibG" + "VSYW5nZU9wAAAAAAAAAAECAAVbAAZlbmRSb3d0AAJbQkwAC25hbWVzcGFjZUlkdAAST"
      + "GphdmEvbGFuZy9TdHJpbmc7TAACb3B0AD1Mb3JnL2FwYWNoZS9hY2N1bXVsby9zZXJ2" + "ZXIvbWFzdGVyL3N0YXRlL01lcmdlSW5mbyRPcGVyYXRpb247WwAIc3RhcnRSb3dxAH4A"
      + "BUwAB3RhYmxlSWRxAH4ABnhyAC5vcmcuYXBhY2hlLmFjY3VtdWxvLm1hc3Rlci50YWJs" + "ZU9wcy5NYXN0ZXJSZXBvAAAAAAAAAAECAAB4cHVyAAJbQqzzF/gGCFTgAgAAeHAAAAAA"
      + "dAAIK2RlZmF1bHR+cgA7b3JnLmFwYWNoZS5hY2N1bXVsby5zZXJ2ZXIubWFzdGVyLnN0" + "YXRlLk1lcmdlSW5mbyRPcGVyYXRpb24AAAAAAAAAABIAAHhyAA5qYXZhLmxhbmcuRW51"
      + "bQAAAAAAAAAAEgAAeHB0AAVNRVJHRXEAfgALdAABMnNyACZvcmcuYXBhY2hlLmFjY3Vt" + "dWxvLnRyYWNlLnRocmlmdC5USW5mb79UcL31bhZ9AwADQgAQX19pc3NldF9iaXRmaWVs"
      + "ZEoACHBhcmVudElkSgAHdHJhY2VJZHhwdwUWABYAAHg=";

  @Test(expected = InvalidClassException.class)
  public void test() throws Exception {
    byte bytes[] = Base64.decodeBase64(oldValue);
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bais);
    ois.readObject();
    fail("did not throw exception");
  }

}
