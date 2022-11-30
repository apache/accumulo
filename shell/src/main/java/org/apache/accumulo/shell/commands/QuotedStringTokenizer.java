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
package org.apache.accumulo.shell.commands;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.accumulo.shell.Shell;

/**
 * A basic tokenizer for generating tokens from a string. It understands quoted strings and escaped
 * quote characters.
 *
 * You can use the escape sequence '\' to escape single quotes, double quotes, and spaces only, in
 * addition to the escape character itself.
 *
 * The behavior is the same for single and double quoted strings. (i.e. '\'' is the same as "\'")
 */
public class QuotedStringTokenizer implements Iterable<String> {
  private ArrayList<String> tokens;
  private String input;

  public QuotedStringTokenizer(final String t) throws BadArgumentException {
    tokens = new ArrayList<>();
    this.input = t;
    try {
      createTokens();
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  public String[] getTokens() {
    return tokens.toArray(new String[tokens.size()]);
  }

  private void createTokens() throws BadArgumentException, UnsupportedEncodingException {
    boolean inQuote = false;
    boolean inEscapeSequence = false;
    String hexChars = null;
    char inQuoteChar = '"';

    final byte[] token = new byte[input.length()];
    int tokenLength = 0;
    final byte[] inputBytes = input.getBytes(UTF_8);
    for (int i = 0; i < input.length(); ++i) {
      final char ch = input.charAt(i);

      // if I ended up in an escape sequence, check for valid escapable character, and add it as a
      // literal
      if (inEscapeSequence) {
        inEscapeSequence = false;
        if (ch == 'x') {
          hexChars = "";
        } else if (ch == ' ' || ch == '\'' || ch == '"' || ch == '\\') {
          token[tokenLength++] = inputBytes[i];
        } else {
          throw new BadArgumentException("can only escape single quotes, double"
              + " quotes, the space character, the backslash, and hex input", input, i);
        }
      } else if (hexChars != null) {
        // in a hex escape sequence
        final int digit = Character.digit(ch, 16);
        if (digit < 0) {
          throw new BadArgumentException("expected hex character", input, i);
        }
        hexChars += ch;
        if (hexChars.length() == 2) {
          byte b;
          try {
            b = (byte) (0xff & Short.parseShort(hexChars, 16));
            if (!Character.isValidCodePoint(0xff & b)) {
              throw new NumberFormatException();
            }
          } catch (NumberFormatException e) {
            throw new BadArgumentException("unsupported non-ascii character", input, i);
          }
          token[tokenLength++] = b;
          hexChars = null;
        }
      } else if (inQuote) {
        // in a quote, either end the quote, start escape, or continue a token
        if (ch == inQuoteChar) {
          inQuote = false;
          tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
          tokenLength = 0;
        } else if (ch == '\\') {
          inEscapeSequence = true;
        } else {
          token[tokenLength++] = inputBytes[i];
        }
      } else {
        // not in a quote, either enter a quote, end a token, start escape, or continue a token
        if (ch == '\'' || ch == '"') {
          if (tokenLength > 0) {
            tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
            tokenLength = 0;
          }
          inQuote = true;
          inQuoteChar = ch;
        } else if (ch == ' ' && tokenLength > 0) {
          tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
          tokenLength = 0;
        } else if (ch == '\\') {
          inEscapeSequence = true;
        } else if (ch != ' ') {
          token[tokenLength++] = inputBytes[i];
        }
      }
    }
    if (inQuote) {
      throw new BadArgumentException("missing terminating quote", input, input.length());
    } else if (inEscapeSequence || hexChars != null) {
      throw new BadArgumentException("escape sequence not complete", input, input.length());
    }
    if (tokenLength > 0) {
      tokens.add(new String(token, 0, tokenLength, Shell.CHARSET));
    }
  }

  @Override
  public Iterator<String> iterator() {
    return tokens.iterator();
  }
}
