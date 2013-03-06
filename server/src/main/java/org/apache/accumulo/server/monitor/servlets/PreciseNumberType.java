package org.apache.accumulo.server.monitor.servlets;

import org.apache.accumulo.server.monitor.util.celltypes.NumberType;

public class PreciseNumberType extends NumberType<Integer> {

  public PreciseNumberType(int warnMin, int warnMax, int errMin, int errMax) {
    super(warnMin, warnMax, errMin, errMax);
  }

  public PreciseNumberType() {
  }

  public static String bigNumber(long big, String[] SUFFIXES, long base) {
    return String.format("%,d", big);
  }
}
