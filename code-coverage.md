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

This branch has changes for code coverage that include server side processes started by mini accumulo. Running an IT like below generate reports.

```bash
mvn clean -PskipQA -DskipTests=false -DskipITs=false -Dit.test=ComprehensiveIT -Dtest=nothing -DfailIfNoTests=false verify
```

Report can be found at `test/target/site/jacoco-aggregate/index.html`.  If using intellij can load the coverage data from `test/target/jacoco-it.exec` as outlined [here](https://www.jetbrains.com/help/idea/code-coverage.html#read_the_coverage_data).

