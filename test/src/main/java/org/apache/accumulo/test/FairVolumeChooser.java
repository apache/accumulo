/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.accumulo.core.spi.fs.VolumeChooser;
import org.apache.accumulo.core.spi.fs.VolumeChooserEnvironment;

/**
 * Try to assign some fairness to choosing Volumes. Intended for tests, not for production
 */
public class FairVolumeChooser implements VolumeChooser {

  private final ConcurrentHashMap<Integer,Integer> optionLengthToLastChoice =
      new ConcurrentHashMap<>();

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options) {
    int currentChoice;
    String[] optionsArray = options.toArray(new String[0]);
    Integer lastChoice = optionLengthToLastChoice.get(optionsArray.length);
    if (lastChoice == null) {
      currentChoice = 0;
    } else {
      currentChoice = lastChoice + 1;
      if (currentChoice >= optionsArray.length) {
        currentChoice = 0;
      }
    }

    optionLengthToLastChoice.put(optionsArray.length, currentChoice);

    return optionsArray[currentChoice];
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options) {
    return options;
  }
}
