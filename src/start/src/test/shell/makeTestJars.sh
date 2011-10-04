#! /bin/sh

cd $ACCUMULO_HOME/src/start
for x in A B C
do
    sed "s/testX/test$x/" < src/test/java/test/TestTemplate > src/test/java/test/TestObject.java
    mkdir -p target/$x
    javac -cp src/test/java src/test/java/test/TestObject.java -d target/$x
    jar -cf src/test/resources/ClassLoaderTest$x/Test.jar -C target/$x test/TestObject.class
    rm -f src/test/java/test/TestObject.java
done
