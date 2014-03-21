Test Concurrent Read/Write
==========================

Can run this test with pre-existing splits; use the following command to create the table with
100 pre-existing splits:

> `$ ../../../bin/accumulo org.apache.accumulo.test.TestIngest --createTable \   
-u root -p secret --splits 100 --rows 0  
$ . concurrent.sh`

