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
package org.apache.accumulo.core.file.rfile;

/**
 * Class that holds the components of a visibility metric. The String visibility, the number of
 * times that is seen in a locality group, the percentage of keys that contain that visibility in
 * the locality group, the number of blocks in the locality group that contain the visibility, and
 * the percentage of blocks in the locality group that contain the visibility.
 */
public class VisibilityMetric {

  private long visLG, visBlock;
  private double visLGPer, visBlockPer;
  private String visibility;

  public VisibilityMetric(String visibility, long visLG, double visLGPer, long visBlock,
      double visBlockPer) {
    this.visibility = visibility;
    this.visLG = visLG;
    this.visLGPer = visLGPer;
    this.visBlock = visBlock;
    this.visBlockPer = visBlockPer;
  }

  /**
   * @return the visibility
   */
  public String getVisibility() {
    return visibility;
  }

  /**
   * @return the visLG
   */
  public long getVisLG() {
    return visLG;
  }

  /**
   * @return the visBlock
   */
  public long getVisBlock() {
    return visBlock;
  }

  /**
   * @return the visLGPer
   */
  public double getVisLGPer() {
    return visLGPer;
  }

  /**
   * @return the visBlockPer
   */
  public double getVisBlockPer() {
    return visBlockPer;
  }

}
