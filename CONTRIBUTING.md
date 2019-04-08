<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Contributors Guide

 If you believe that you have found a bug, please search for an existing [issue](https://github.com/apache/accumulo/issues) to see if it has already been reported. If you would like to add a new feature to Accumulo, please send an email with your idea to the [dev](mailto:dev@accumulo.apache.org) mail list. If it's appropriate, then we will create an issue.  For simple changes, its ok to just submit a pull request without an issue.

## Development

- See the [Developer's Guide](https://accumulo.apache.org/how-to-contribute/) for information regarding common build commands, IDE setup and more.
- Ensure that your work targets the correct branch
- Add / update unit and integration tests

## Patch Submission

- Ensure that Accumulo builds cleanly before submitting your patch using the command: `mvn clean verify -DskipITs`
- Before submission please squash your commits using a message that starts with the issue number and a description of the changes.
- Patches should be submitted in the form of Pull Requests to the Apache Accumulo GitHub [repository](https://github.com/apache/accumulo/) or to the [Review Board](https://reviews.apache.org) accumulo repository.

## Review

- We welcome reviews from anyone. Any committer can approve and merge the changes.
- Reviewers will likely have questions and comments. They may use terms such as those in [RFC2119](https://tools.ietf.org/html/rfc2119).

