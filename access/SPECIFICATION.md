<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# AccessExpression Specification

This document specifies the format of an Apache Accumulo AccessExpression. An AccessExpression
is an encoding of a boolean expression of the attributes that a subject is required to have to
access a particular piece of data.

## Syntax

The formal definition of the AccessExpression UTF-8 string representation is provided by
the following [ABNF][1]:

```
access_expression       = [expression] ; empty string is a valid access expression

expression              =  and_expression / or_expression

and_expression          =  and_expression and_operator and_expression
and_expression          =/ lparen expression rparen
and_expression          =/ access_token 

or_expression           =  or_expression or_operator or_expression
or_expression           =/ lparen expression rparen
or_expression           =/ access_token 

access_token            = 1*( ALPHA / DIGIT / "_" / "-" / "." / ":" / slash )
access_token            =/ DQUOTE 1*(utf8_subset / escaped) DQUOTE

utf8_subset             = %x20-21 / %x23-5B / %5D-7E / UVCHARBEYONDASCII ; utf8 minus '"' and '\'
escaped                 = "\" DQUOTE / "\\"
slash                   = "/"
or_operator             = "|"
and_operator            = "&"
lparen                  = "("
rparen                  = ")"
```

The definition of utf8 was borrowed from this [ietf document][2].  TODO that doc defines unicode and not utf8

## Serialization

An AccessExpression is a UTF-8 string. It can be serialized using a byte array as long as it
can be deserialized back into the same UTF-8 string.

[1]: https://www.rfc-editor.org/rfc/rfc5234
[2]: https://datatracker.ietf.org/doc/html/draft-seantek-unicode-in-abnf-03#section-4.2
