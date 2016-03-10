---
title: Terasort Example
---

This example uses map/reduce to generate random input data that will
be sorted by storing it into accumulo.  It uses data very similar to the
hadoop terasort benchmark.

To run this example you run it with arguments describing the amount of data:

    $ bin/tool.sh lib/accumulo-examples-simple.jar org.apache.accumulo.examples.simple.mapreduce.TeraSortIngest \
    -i instance -z zookeepers -u user -p password \
    --count 10 \
    --minKeySize 10 \ 
    --maxKeySize 10 \
    --minValueSize 78 \
    --maxValueSize 78 \
    --table sort \
    --splits 10 \

After the map reduce job completes, scan the data:

    $ ./bin/accumulo shell -u username -p password
    username@instance> scan -t sort 
    +l-$$OE/ZH c:         4 []    GGGGGGGGGGWWWWWWWWWWMMMMMMMMMMCCCCCCCCCCSSSSSSSSSSIIIIIIIIIIYYYYYYYYYYOOOOOOOO
    ,C)wDw//u= c:        10 []    CCCCCCCCCCSSSSSSSSSSIIIIIIIIIIYYYYYYYYYYOOOOOOOOOOEEEEEEEEEEUUUUUUUUUUKKKKKKKK
    75@~?'WdUF c:         1 []    IIIIIIIIIIYYYYYYYYYYOOOOOOOOOOEEEEEEEEEEUUUUUUUUUUKKKKKKKKKKAAAAAAAAAAQQQQQQQQ
    ;L+!2rT~hd c:         8 []    MMMMMMMMMMCCCCCCCCCCSSSSSSSSSSIIIIIIIIIIYYYYYYYYYYOOOOOOOOOOEEEEEEEEEEUUUUUUUU
    LsS8)|.ZLD c:         5 []    OOOOOOOOOOEEEEEEEEEEUUUUUUUUUUKKKKKKKKKKAAAAAAAAAAQQQQQQQQQQGGGGGGGGGGWWWWWWWW
    M^*dDE;6^< c:         9 []    UUUUUUUUUUKKKKKKKKKKAAAAAAAAAAQQQQQQQQQQGGGGGGGGGGWWWWWWWWWWMMMMMMMMMMCCCCCCCC
    ^Eu)<n#kdP c:         3 []    YYYYYYYYYYOOOOOOOOOOEEEEEEEEEEUUUUUUUUUUKKKKKKKKKKAAAAAAAAAAQQQQQQQQQQGGGGGGGG
    le5awB.$sm c:         6 []    WWWWWWWWWWMMMMMMMMMMCCCCCCCCCCSSSSSSSSSSIIIIIIIIIIYYYYYYYYYYOOOOOOOOOOEEEEEEEE
    q__[fwhKFg c:         7 []    EEEEEEEEEEUUUUUUUUUUKKKKKKKKKKAAAAAAAAAAQQQQQQQQQQGGGGGGGGGGWWWWWWWWWWMMMMMMMM
    w[o||:N&H, c:         2 []    QQQQQQQQQQGGGGGGGGGGWWWWWWWWWWMMMMMMMMMMCCCCCCCCCCSSSSSSSSSSIIIIIIIIIIYYYYYYYY

Of course, a real benchmark would ingest millions of entries.
