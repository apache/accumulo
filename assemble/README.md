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

# Accumulo Completions

The `_accumulo_completions` script, located in `assemble/bin` is a BASH completion script for the `accumulo`, `accumulo admin`, and `accumulo ec-admin` commands.

## Setup

Clone [Apache Accumulo](https://github.com/apache/accumulo) locally onto your system, and run an instance of Accumulo. From within your clone of Accumulo, source the `_accumulo_completions` script to be able to use the bash completions, like so:

```shell
  source assemble/bin/_accumulo_completions
```

## Usage

Begin typing in commands, hitting `[TAB]` twice to complete commands or to show available commands.