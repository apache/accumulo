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
package org.apache.accumulo.server.tabletserver;

import java.math.BigInteger;

import org.apache.log4j.Logger;

/**
 * This class provides a way of encoding a path into a binary tree into a string.
 * 
 * The encoding is constructed in a such a way that siblings should come togther when the encodings are sorted.
 * 
 * 
 * 
 */

public class EncodedBinaryTreePath {
  
  private int len;
  private BigInteger path;
  private static final Logger log = Logger.getLogger(EncodedBinaryTreePath.class);
  
  public EncodedBinaryTreePath() {
    len = 0;
    path = BigInteger.ZERO;
  }
  
  public EncodedBinaryTreePath(String encodedPath) {
    
    String pathString = encodedPath;
    String hexPath = pathString.substring(0, pathString.indexOf('-'));
    
    int zIndex = hexPath.indexOf('.');
    
    if (zIndex >= 0) {
      hexPath = hexPath.substring(0, zIndex);
    }
    
    StringBuilder sb = new StringBuilder();
    for (int i = hexPath.length() - 1; i >= 0; i--) {
      sb.append(hexPath.charAt(i));
    }
    
    if (sb.length() == 0) {
      path = new BigInteger("0", 16);
    } else {
      path = new BigInteger(sb.toString(), 16);
    }
    
    String binaryPath = pathString.substring(pathString.indexOf('-') + 1);
    
    int startBit = hexPath.length() * 4;
    
    for (int i = 0; i < binaryPath.length(); i++) {
      
      if (binaryPath.charAt(i) == '.') {
        continue;
      }
      
      if (binaryPath.charAt(i) == '0') {
        path = path.clearBit(startBit++);
      } else {
        path = path.setBit(startBit++);
      }
    }
    
    len = startBit;
    
  }
  
  public EncodedBinaryTreePath getParent() {
    
    if (len == 0) {
      throw new RuntimeException("Can not get parent of root");
    }
    
    EncodedBinaryTreePath ebtp = new EncodedBinaryTreePath();
    ebtp.path = path.clearBit(len - 1);
    ebtp.len = len - 1;
    return ebtp;
  }
  
  public EncodedBinaryTreePath getLeftChild() {
    EncodedBinaryTreePath ebtp = new EncodedBinaryTreePath();
    ebtp.path = path.clearBit(len);
    ebtp.len = len + 1;
    return ebtp;
  }
  
  public EncodedBinaryTreePath getRightChild() {
    EncodedBinaryTreePath ebtp = new EncodedBinaryTreePath();
    ebtp.path = path.setBit(len);
    ebtp.len = len + 1;
    return ebtp;
  }
  
  public String getEncoding() {
    StringBuilder sb = new StringBuilder();
    
    String hexPath = path.toString(16);
    
    int expectedLen = len / 4 + (len % 4 > 0 ? 1 : 0);
    
    if (hexPath.length() < expectedLen) {
      StringBuilder sb2 = new StringBuilder();
      while (sb2.length() + hexPath.length() < expectedLen) {
        sb2.append('0');
      }
      sb2.append(hexPath);
      hexPath = sb2.toString();
    }
    
    for (int i = 0; i < (len - 1) / 4; i++) {
      int index = hexPath.length() - i - 1;
      sb.append(hexPath.charAt(index));
    }
    
    while (sb.length() < 16) {
      sb.append('.');
    }
    
    sb.append('-');
    
    if (len % 4 > 0 || len == 0)
      for (int i = 0; i < 4 - (len % 4); i++) {
        sb.append('.');
      }
    
    int startBit = ((len - 1) / 4) * 4;
    for (int i = startBit; i < len; i++) {
      sb.append(path.testBit(i) ? "1" : "0");
    }
    
    return sb.toString();
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("R");
    for (int i = 0; i < len; i++) {
      sb.append(path.testBit(i) ? "1" : "0");
    }
    
    return sb.toString();
  }
  
  public static void main(String[] args) {
    for (int len = 0; len < 22; len++) {
      
      for (int i = 0; i < 1 << len; i++) {
        EncodedBinaryTreePath ebtp = new EncodedBinaryTreePath();
        StringBuilder sb = new StringBuilder();
        sb.append('R');
        for (int j = 0; j < len; j++) {
          if (((i >> j) & 0x1) == 0) {
            sb.append('0');
            ebtp = ebtp.getLeftChild();
          } else {
            sb.append('1');
            ebtp = ebtp.getRightChild();
          }
        }
        
        if (!sb.toString().equals(ebtp.toString())) {
          
          log.info(sb.toString());
          log.info(ebtp);
        }
        ebtp = new EncodedBinaryTreePath(ebtp.getEncoding());
        
        if (!sb.toString().equals(ebtp.toString())) {
          log.error("Encoding failed");
          log.error(sb.toString());
          log.error(ebtp);
        }
      }
    }
  }
  
}
