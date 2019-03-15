package org.apache.accumulo.master.metrics.fate;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class FateMetricsTest {


  @Test public void defaultValueTest() {

    FateMetrics.FateMetricValues.Builder builder = FateMetrics.FateMetricValues.Builder.getBuilder();

    FateMetrics.FateMetricValues v = builder.build();

    assertEquals(0, v.getCurrentFateOps());
    assertEquals(0, v.getLastFateZxid());
    assertEquals(0, v.getZkConnectionErrors());

  }

  @Test public void valueTest() {

    FateMetrics.FateMetricValues.Builder builder = FateMetrics.FateMetricValues.Builder.getBuilder();

    FateMetrics.FateMetricValues v = builder.withCurrentFateOps(1)
        .withLastFateZxid(2)
        .withZkConnectionErrors(3).build();

    assertEquals(1, v.getCurrentFateOps());
    assertEquals(2, v.getLastFateZxid());
    assertEquals(3, v.getZkConnectionErrors());

    FateMetrics.FateMetricValues.Builder builder2 = FateMetrics.FateMetricValues.Builder.copy(v);

    FateMetrics.FateMetricValues v2 = builder2.withCurrentFateOps(11).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(2, v2.getLastFateZxid());
    assertEquals(3, v2.getZkConnectionErrors());

    v2 = builder2.withLastFateZxid(22).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getLastFateZxid());
    assertEquals(3, v2.getZkConnectionErrors());

    v2 = builder2.withZkConnectionErrors(33).build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getLastFateZxid());
    assertEquals(33, v2.getZkConnectionErrors());

    v2 = builder2.incrZkConnectionErrors().build();

    assertEquals(11, v2.getCurrentFateOps());
    assertEquals(22, v2.getLastFateZxid());
    assertEquals(34, v2.getZkConnectionErrors());

  }
}
