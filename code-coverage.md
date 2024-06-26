
To get code coverage for serverside processed started by Mini Accumulo need to
set the following env variable before running maven.  Mini Accumulo will set
`MAC_JACOCO` as a jvm argument on java processes it starts.  Mini Accumulo will
replace `processName` with unique information so that a file per process is
created.  May need to change the jacoco version to match what is in the pom.

```bash
MAVEN_REPO=$HOME/.m2/repository
SOURCE_DIR=<dir where your accumulo source is>
export MAC_JACOCO="-javaagent:$MAVEN_REPO/org/jacoco/org.jacoco.agent/0.8.12/org.jacoco.agent-0.8.12-runtime.jar=destfile=$SOURCE_DIR/accumulo/test/target/jacoco-processName.exec"
```

With the above set can use a command like the following to run an IT and see
client and server side code coverage.  Look under `accumulo/test/target/site/`

```baash
mvn clean -PskipQA -DskipTests=false -DskipITs=false -Dit.test=ComprehensiveIT -Dtest=nothing -DfailIfNoTests=false verify
```

