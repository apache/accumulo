/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.iteratortest;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.iteratortest.testcases.IteratorTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

/**
 * A class to ease finding published test cases.
 */
public class IteratorTestCaseFinder {
  private static final Logger log = LoggerFactory.getLogger(IteratorTestCaseFinder.class);

  /**
   * Instantiates all test cases provided.
   *
   * @return A list of {@link IteratorTestCase}s.
   */
  public static List<IteratorTestCase> findAllTestCases() {
    log.info("Searching {}", IteratorTestCase.class.getPackage().getName());
    ClassPath cp;
    try {
      cp = ClassPath.from(IteratorTestCaseFinder.class.getClassLoader());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    ImmutableSet<ClassInfo> classes = cp.getTopLevelClasses(IteratorTestCase.class.getPackage().getName());

    final List<IteratorTestCase> testCases = new ArrayList<>();
    // final Set<Class<? extends IteratorTestCase>> classes = reflections.getSubTypesOf(IteratorTestCase.class);
    for (ClassInfo classInfo : classes) {
      Class<?> clz;
      try {
        clz = Class.forName(classInfo.getName());
      } catch (Exception e) {
        log.warn("Could not get class for " + classInfo.getName(), e);
        continue;
      }

      if (clz.isInterface() || Modifier.isAbstract(clz.getModifiers()) || !IteratorTestCase.class.isAssignableFrom(clz)) {
        log.debug("Skipping " + clz);
        continue;
      }

      try {
        testCases.add((IteratorTestCase) clz.newInstance());
      } catch (IllegalAccessException | InstantiationException e) {
        log.warn("Could not instantiate {}", clz, e);
      }
    }

    return testCases;
  }
}
