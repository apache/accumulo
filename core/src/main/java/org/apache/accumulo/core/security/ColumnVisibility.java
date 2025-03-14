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
package org.apache.accumulo.core.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.access.InvalidAccessExpressionException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.hadoop.io.Text;

/**
 * Validate the column visibility is a valid expression and set the visibility for a Mutation. See
 * {@link ColumnVisibility#ColumnVisibility(byte[])} for the definition of an expression.
 *
 * <p>
 * The expression is a sequence of characters from the set [A-Za-z0-9_-.] along with the binary
 * operators "&amp;" and "|" indicating that both operands are necessary, or the either is
 * necessary. The following are valid expressions for visibility:
 *
 * <pre>
 * A
 * A|B
 * (A|B)&amp;(C|D)
 * orange|(red&amp;yellow)
 * </pre>
 *
 * <p>
 * The following are not valid expressions for visibility:
 *
 * <pre>
 * A|B&amp;C
 * A=B
 * A|B|
 * A&amp;|B
 * ()
 * )
 * dog|!cat
 * </pre>
 *
 * <p>
 * In addition to the base set of visibilities, any character can be used in the expression if it is
 * quoted. If the quoted term contains '&quot;' or '\', then escape the character with '\'. The
 * {@link AccessExpression#quote(String)} method can be used to properly quote and escape terms
 * automatically. The following is an example of a quoted term:
 *
 * <pre>
 * &quot;A#C&quot; &amp; B
 * </pre>
 */
public class ColumnVisibility {
  private final byte[] expression;

  /**
   * Accessor for the underlying byte string.
   *
   * @return byte array representation of a visibility expression
   */
  public byte[] getExpression() {
    return expression;
  }

  private static final byte[] EMPTY_BYTES = new byte[0];

  /**
   * Creates an empty visibility. Normally, elements with empty visibility can be seen by everyone.
   * Though, one could change this behavior with filters.
   *
   * @see #ColumnVisibility(String)
   */
  public ColumnVisibility() {
    expression = EMPTY_BYTES;
  }

  /**
   * Creates a column visibility for a Mutation.
   *
   * @param expression An expression of the rights needed to see this mutation. The expression
   *        syntax is defined at the class-level documentation
   */
  public ColumnVisibility(String expression) {
    this(expression.getBytes(UTF_8));
  }

  /**
   * Creates a column visibility for a Mutation.
   *
   * @param expression visibility expression
   * @see #ColumnVisibility(String)
   */
  public ColumnVisibility(Text expression) {
    this(TextUtil.getBytes(expression));
  }

  /**
   * Creates a column visibility for a Mutation from bytes already encoded in UTF-8.
   *
   * @param expression visibility expression, encoded as UTF-8 bytes
   * @see #ColumnVisibility(String)
   */
  public ColumnVisibility(byte[] expression) {
    this.expression = expression;
    try {
      AccessExpression.validate(this.expression);
    } catch (InvalidAccessExpressionException e) {
      // This is thrown for compatability with the exception this class used to throw when it parsed
      // exceptions itself.
      throw new BadArgumentException(e);
    }
  }

  /**
   * Creates a column visibility for a Mutation from an AccessExpression.
   *
   * @param expression visibility expression, encoded as UTF-8 bytes
   * @see #ColumnVisibility(String)
   * @since 3.1.0
   */
  public ColumnVisibility(AccessExpression expression) {
    // AccessExpression is a validated immutable object, so no need to re validate
    this.expression = expression.getExpression().getBytes(UTF_8);
  }

  @Override
  public String toString() {
    return "[" + new String(expression, UTF_8) + "]";
  }

  /**
   * See {@link #equals(ColumnVisibility)}
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ColumnVisibility) {
      return equals((ColumnVisibility) obj);
    }
    return false;
  }

  /**
   * Compares two ColumnVisibilities for string equivalence, not as a meaningful comparison of terms
   * and conditions.
   *
   * @param otherLe other column visibility
   * @return true if this visibility equals the other via string comparison
   */
  public boolean equals(ColumnVisibility otherLe) {
    return Arrays.equals(expression, otherLe.expression);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(expression);
  }
}
