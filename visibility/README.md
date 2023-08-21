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

# accumulo-visibility
Prototype of standalone Accumulo Visibility Library

Goals of this prototype.

 * Create a standalone java library that offers the Accumulo visibility functionality
 * Have no dependencies for this new library
 * Adapt Accumulo to use this library behind the scenes for its implementation of ColumnVisibility and VisibilityEvaluator

The following types constitute the public API of this library.  All other types are package private and are not part of the public API.

  * [VisibilityArbiter](src/main/java/org/apache/accumulo/visibility/VisibilityArbiter.java).
  * [VisibilityExpression](src/main/java/org/apache/accumulo/visibility/VisibilityExpression.java).

For an example of using this library see the [unit test](src/test/java/org/apache/accumulo/visibility/VisibilityArbiterTest.java).
