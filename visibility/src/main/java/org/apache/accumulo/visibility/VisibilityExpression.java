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
package org.apache.accumulo.visibility;

import java.util.Set;

/**
 * An opaque type that contains a parsed visibility expression. When this type is constructed with
 * {@link #parse(String)} and then used with
 * {@link VisibilityArbiter#isVisible(VisibilityExpression)} it can be more efficient and avoid
 * reparsing the expression.
 *
 * <p>
 * For reviewers : this type is similar to ColumnVisibility. This interface and impl have goal of
 * being immutable which differs from column visibility. ColumnVisibility leaks internal
 * implementation details in its public API, this type does not.
 *
 * TODO needs better javadoc.
 *
 * Below is an example of using this API.
 *
 * <pre>
 *     {@code
 * var auth1 = VisibilityExpression.quote("CAT");
 * var auth2 = VisibilityExpression.quote("🦕");
 * var auth3 = VisibilityExpression.quote("🦖");
 * var visExp = VisibilityExpression
 *     .parse("(" + auth1 + "&" + auth3 + ")|(" + auth1 + "&" + auth2 + "&" + auth1 + ")");
 * System.out.println(visExp.getExpression());
 * System.out.println(visExp.normalize());
 * System.out.println(visExp.getAuthorizations());
 * }
 * </pre>
 *
 * The above example will print the following.
 *
 * <pre>
 * (CAT&amp;"🦖")|(CAT&amp;"🦕"&CAT)
 * ("🦕"&amp;CAT)|("🦖"&amp;CAT)
 * [🦖, CAT, 🦕]
 * </pre>
 *
 * @since ???
 */
// TODO could name VisibilityLabel
public interface VisibilityExpression {

  /**
   * @return the expression that was used to create this object.
   */
  String getExpression();

  /**
   * TODO give examples
   *
   * @return A normalized version of the visibility expression that removes duplicates and orders
   *         the expression in a consistent way.
   */
  String normalize();

  /**
   * @return the authorizations that occur in the expression. For example, for the expression
   *         {@code (A&B)|(A&C)|(A&D)} this method would return {@code [A,B,C,D]]}
   */
  Set<String> getAuthorizations();

  // TODO builder instead? or use name of()?
  static VisibilityExpression parse(String expression) throws IllegalVisibilityException {
    return new VisibilityExpressionImpl(expression);
  }

  // TODO builder instead? or use name of()?
  // TODO document utf8 expectations
  static VisibilityExpression parse(byte[] expression) throws IllegalVisibilityException {
    return new VisibilityExpressionImpl(expression);
  }

  /**
   * Authorizations occurring a visibility expression can only contain the characters TODO unless
   * quoted. Use this method to quote authorizations that occur in a visibility expression. This
   * method will quote if its needed.
   */
  static byte[] quote(byte[] authorization) {
    return VisibilityExpressionImpl.quote(authorization);
  }

  /**
   * Authorizations occurring a visibility expression can only contain the characters TODO unless
   * quoted. Use this method to quote authorizations that occur in a visibility expression. This
   * method will quote if its needed.
   */
  static String quote(String authorization) {
    return VisibilityExpressionImpl.quote(authorization);
  }

  /**
   * @return an empty VisibilityExpression.
   */
  // TODO could name of()
  static VisibilityExpression empty() {
    return VisibilityExpressionImpl.EMPTY;
  }
}
