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
package org.apache.accumulo.examples.wikisearch.jexl;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.jexl2.JexlArithmetic;
import org.apache.commons.lang.math.NumberUtils;

public class Arithmetic extends JexlArithmetic {
  
  public Arithmetic(boolean lenient) {
    super(lenient);
  }
  
  /**
   * This method differs from the parent in that we are not calling String.matches() because it does not match on a newline. Instead we are handling this case.
   * 
   * @param left
   *          first value
   * @param right
   *          second value
   * @return test result.
   */
  @Override
  public boolean matches(Object left, Object right) {
    if (left == null && right == null) {
      // if both are null L == R
      return true;
    }
    if (left == null || right == null) {
      // we know both aren't null, therefore L != R
      return false;
    }
    final String arg = left.toString();
    if (right instanceof java.util.regex.Pattern) {
      return ((java.util.regex.Pattern) right).matcher(arg).matches();
    } else {
      // return arg.matches(right.toString());
      Pattern p = Pattern.compile(right.toString(), Pattern.DOTALL);
      Matcher m = p.matcher(arg);
      return m.matches();
      
    }
  }
  
  /**
   * This method differs from the parent class in that we are going to try and do a better job of coercing the types. As a last resort we will do a string
   * comparison and try not to throw a NumberFormatException. The JexlArithmetic class performs coercion to a particular type if either the left or the right
   * match a known type. We will look at the type of the right operator and try to make the left of the same type.
   */
  @Override
  public boolean equals(Object left, Object right) {
    Object fixedLeft = fixLeft(left, right);
    return super.equals(fixedLeft, right);
  }
  
  @Override
  public boolean lessThan(Object left, Object right) {
    Object fixedLeft = fixLeft(left, right);
    return super.lessThan(fixedLeft, right);
  }
  
  protected Object fixLeft(Object left, Object right) {
    
    if (null == left || null == right)
      return left;
    
    if (!(right instanceof Number) && left instanceof Number) {
      right = NumberUtils.createNumber(right.toString());
    }
    
    if (right instanceof Number && left instanceof Number) {
      if (right instanceof Double)
        return ((Double) right).doubleValue();
      else if (right instanceof Float)
        return ((Float) right).floatValue();
      else if (right instanceof Long)
        return ((Long) right).longValue();
      else if (right instanceof Integer)
        return ((Integer) right).intValue();
      else if (right instanceof Short)
        return ((Short) right).shortValue();
      else if (right instanceof Byte)
        return ((Byte) right).byteValue();
      else
        return right;
    }
    if (right instanceof Number && left instanceof String) {
      Number num = NumberUtils.createNumber(left.toString());
      // Let's try to cast left as right's type.
      if (this.isFloatingPointNumber(right) && this.isFloatingPointNumber(left))
        return num;
      else if (this.isFloatingPointNumber(right))
        return num.doubleValue();
      else if (right instanceof Number)
        return num.longValue();
    } else if (right instanceof Boolean && left instanceof String) {
      if (left.equals("true") || left.equals("false"))
        return Boolean.parseBoolean(left.toString());
      
      Number num = NumberUtils.createNumber(left.toString());
      if (num.intValue() == 1)
        return (Boolean) true;
      else if (num.intValue() == 0)
        return (Boolean) false;
    }
    return left;
  }
  
}
