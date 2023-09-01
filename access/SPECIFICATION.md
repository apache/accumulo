# AccessExpression Specification

This document specifies the format of an Apache Accumulo AccessExpression. An AccessExpression
is an encoding of a boolean expression of the attributes that a subject is required to have to
access a particular piece of data.

## Syntax

The formal definition of the AccessExpression UTF-8 string representation is provided by
the following ABNF[1]:

```
access_expression       =  and_expression / or_expression

and_expression          =  and_expression and_operator and_expression
and_expression          =/ lparen access_expression rparen
and_expression          =/ access_token 

or_expression           =  or_expression or_operator or_expression
or_expression           =/ lparen access_expression rparen
or_expression           =/ access_token 

access_token            = 1*( ALPHA / DIGIT / underscore / hyphen / period / colon / slash )
access_token            =/ DQUOTE *utf8 DQUOTE ; TODO define escaping

utf8                    = %x20-21 / %x23-7E / UVCHARBEYONDASCII ; see [2]
underscore              = "_"
hyphen                  = "-"
period                  = "."
colon                   = ":"
slash                   = "/"
or_operator             = "|"
and_operator            = "&"
lparen                  = "("
rparen                  = ")"
```

## Serialization

An AccessExpression is a UTF-8 string. It can be serialized using a byte array as long as it
can be deserialized back into the same UTF-8 string.

[1]: https://www.rfc-editor.org/rfc/rfc5234
[2]: https://datatracker.ietf.org/doc/html/draft-seantek-unicode-in-abnf-03#section-4.2
