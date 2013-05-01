package org.apache.accumulo.core.util.shell;

import static org.junit.Assert.assertEquals;

import org.apache.accumulo.core.util.TableDiskUsage;
import org.junit.Test;

public class TableDiskUsageTest {
  
  @Test
  public void testHumanReadableBytes() {
    assertEquals("1000B", TableDiskUsage.humanReadableBytes(1000));
    assertEquals(" 1.0K", TableDiskUsage.humanReadableBytes(1024));
    assertEquals(" 1.5K", TableDiskUsage.humanReadableBytes(1024 + (1024 / 2)));
    assertEquals("1024K", TableDiskUsage.humanReadableBytes(1024 * 1024 - 1));
    assertEquals(" 1.0M", TableDiskUsage.humanReadableBytes(1024 * 1024));
    assertEquals(" 1.5M", TableDiskUsage.humanReadableBytes(1024 * 1024 + (1024 * 1024 / 2)));
    assertEquals("1024M", TableDiskUsage.humanReadableBytes(1073741823));
    assertEquals(" 1.0G", TableDiskUsage.humanReadableBytes(1073741824));
    assertEquals("1024G", TableDiskUsage.humanReadableBytes(1099511627775l));
    assertEquals(" 1.0T", TableDiskUsage.humanReadableBytes(1099511627776l));
  }
}
