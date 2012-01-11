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
package org.apache.accumulo.examples.wikisearch.normalizer;

import static org.junit.Assert.assertTrue;

import org.apache.accumulo.examples.wikisearch.normalizer.NumberNormalizer;
import org.junit.Test;

public class testNumberNormalizer {
  
  @Test
  public void test1() throws Exception {
    NumberNormalizer nn = new NumberNormalizer();
    
    String n1 = nn.normalizeFieldValue(null, "1");
    String n2 = nn.normalizeFieldValue(null, "1.00000000");
    
    assertTrue(n1.compareTo(n2) < 0);
    
  }
  
  @Test
  public void test2() {
    NumberNormalizer nn = new NumberNormalizer();
    
    String n1 = nn.normalizeFieldValue(null, "-1.0");
    String n2 = nn.normalizeFieldValue(null, "1.0");
    
    assertTrue(n1.compareTo(n2) < 0);
    
  }
  
  @Test
  public void test3() {
    NumberNormalizer nn = new NumberNormalizer();
    String n1 = nn.normalizeFieldValue(null, "-0.0001");
    String n2 = nn.normalizeFieldValue(null, "0");
    String n3 = nn.normalizeFieldValue(null, "0.00001");
    
    assertTrue((n1.compareTo(n2) < 0) && (n2.compareTo(n3) < 0));
  }
  
  @Test
  public void test4() {
    NumberNormalizer nn = new NumberNormalizer();
    String nn1 = nn.normalizeFieldValue(null, Integer.toString(Integer.MAX_VALUE));
    String nn2 = nn.normalizeFieldValue(null, Integer.toString(Integer.MAX_VALUE - 1));
    
    assertTrue((nn2.compareTo(nn1) < 0));
    
  }
  
  @Test
  public void test5() {
    NumberNormalizer nn = new NumberNormalizer();
    String nn1 = nn.normalizeFieldValue(null, "-0.001");
    String nn2 = nn.normalizeFieldValue(null, "-0.0009");
    String nn3 = nn.normalizeFieldValue(null, "-0.00090");
    
    assertTrue((nn3.compareTo(nn2) == 0) && (nn2.compareTo(nn1) > 0));
    
  }
  
  @Test
  public void test6() {
    NumberNormalizer nn = new NumberNormalizer();
    String nn1 = nn.normalizeFieldValue(null, "00.0");
    String nn2 = nn.normalizeFieldValue(null, "0");
    String nn3 = nn.normalizeFieldValue(null, "0.0");
    
    assertTrue((nn3.compareTo(nn2) == 0) && (nn2.compareTo(nn1) == 0));
    
  }
  
}
