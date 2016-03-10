---
title: Isolation Example
---

This example shows running a simple reservation system implemented using
conditional mutations. This system guarantees that only one concurrent user can
reserve a resource. The example's reserve command allows multiple users to be
specified. When this is done, it creates a separate reservation thread for each
user. In the example below threads are spun up for alice, bob, eve, mallory,
and trent to reserve room06 on 20140101. Bob ends up getting the reservation
and everyone else is put on a wait list. The example code will take any string
for what, when and who.

    $ ./bin/accumulo org.apache.accumulo.examples.simple.reservations.ARS
    >connect test16 localhost root secret ars
      connected
    >
      Commands :
        reserve <what> <when> <who> {who}
        cancel <what> <when> <who>
        list <what> <when>
    >reserve room06 20140101 alice bob eve mallory trent
                       bob : RESERVED
                   mallory : WAIT_LISTED
                     alice : WAIT_LISTED
                     trent : WAIT_LISTED
                       eve : WAIT_LISTED
    >list room06 20140101
      Reservation holder : bob
      Wait list : [mallory, alice, trent, eve]
    >cancel room06 20140101 alice
    >cancel room06 20140101 bob
    >list room06 20140101
      Reservation holder : mallory
      Wait list : [trent, eve]
    >quit

Scanning the table in the Accumulo shell after running the example shows the
following:

    root@test16> table ars
    root@test16 ars> scan
    room06:20140101 res:0001 []    mallory
    room06:20140101 res:0003 []    trent
    room06:20140101 res:0004 []    eve
    room06:20140101 tx:seq []    6

The tx:seq column is incremented for each update to the row allowing for
detection of concurrent changes. For an update to go through, the sequence
number must not have changed since the data was read. If it does change,
the conditional mutation will fail and the example code will retry.

