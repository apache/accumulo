---
title: "Balancing Groups of Tablets"
date: 2015-03-20 17:00:00 +0000
author: Keith Turner
---

Originally posted at [https://blogs.apache.org/accumulo/entry/balancing_groups_of_tablets](https://blogs.apache.org/accumulo/entry/balancing_groups_of_tablets)

Accumulo has a pluggable tablet balancer that decides where tablets should be placed. Accumulo's default configuration spreads each tables tablets evenly and randomly across the tablet servers. Each table can configure a custom balancer that does something different.

For some applications to perform optimally, sub-ranges of a table need to be spread evenly across the cluster. Over the years I have run into multiple use cases for this situation. The latest use case was [bad performance][bad-perf] on the [Fluo][fluo] [Stress Test][stress-test]. This test stores a tree in an Accumulo table and creates multiple tablets for each level in the tree. In parallel, the test reads data from one level and writes it up to the next level. Figure 1 below shows an example of tablet servers hosting tablets for different levels of the tree. Under this scenario if many threads are reading data from level 2 and writing up to level 1, only Tserver 1 and Tserver 2 will be utilized. So in this scenario 50% of the tablet servers are idle.

![figure1]({{ site.baseurl}}/images/blog/201503_balancer/figure1.png)
*Figure 1*

[ACCUMULO-3439][accumulo-3949] remedied this situation with the introduction of the [GroupBalancer] and [RegexGroupBalancer] which will be available in Accumulo 1.7.0. These balancers allow a user to arbitrarily group tablets. Each group defined by the user will be evenly spread across the tablet servers. Also, the total number of groups on each tablet server is minimized. As tablets are added or removed from the table, the balancer will migrate tablets to satisfy these goals.  Much of the complexity in the GroupBalancer code comes from trying to minimize the number of migrations needed to reach a good state.

A GroupBalancer could be configured for the table in figure 1 in such a way that it grouped tablets by level. If this were done, the result may look like Figure 2 below. With this tablet to tablet server mapping, many threads reading from level 2 and writing data up to level 1 would utilize all of the tablet servers yielding better performance.

![figure2]({{ site.baseurl}}/images/blog/201503_balancer/figure2.png)
*Figure 2*

[README.rgbalancer][rgbalancer] provides a good example of configuring and using the RegexGroupBalancer. If a regular expression can not accomplish the needed grouping, then a grouping function can be written in Java. Extend GroupBalancer to write a grouping function in java. RegexGroupBalancer provides a good example of how to do this.

When using a GroupBalancer, how Accumulo automatically splits tablets must be kept in mind. When Accumulo decides to split a tablet, it chooses the shortest possible row prefix from the tablet data that yields a good split point. Therefore its possible that a split point that is shorter than what is expected by a GroupBalancer could be chosen. The best way to avoid this situation is to pre-split the table such that it precludes this possibility.

The Fluo Stress test is a very abstract use case. A more concrete use case for the group balancer would be using it to ensure tablets storing geographic data were spread out evenly. For example consider [GeoWave's][geowave] Accumulo [Persistence Model][persis-model]. Tablets could be balanced such that bins related to different regions are spread out evenly. For example tablets related to each continent could be assigned a group ensuring data related to each continent is evenly spread across the cluster. Alternatively, each Tier could spread evenly across the cluster.

[bad-perf]: https://github.com/fluo-io/fluo/issues/361
[fluo]: http://fluo.io/
[stress-test]: https://github.com/fluo-io/fluo-stress
[accumulo-3439]: https://issues.apache.org/jira/browse/ACCUMULO-3439
[rgbalancer]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob;f=docs/src/main/resources/examples/README.rgbalancer;hb=51fbfaf0a52dc89e8294c86c30164fb94c9f644c
[RegexGroupBalancer]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob;f=server/base/src/main/java/org/apache/accumulo/server/master/balancer/RegexGroupBalancer.java;hb=51fbfaf0a52dc89e8294c86c30164fb94c9f644c
[GroupBalancer]: https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob;f=server/base/src/main/java/org/apache/accumulo/server/master/balancer/GroupBalancer.java;hb=b0815affade66ab04ca27b6fc3abaac400097469
[geowave]: https://ngageoint.github.io/geowave/
[persis-model]: http://ngageoint.github.io/geowave/documentation.html#architecture-accumulo
