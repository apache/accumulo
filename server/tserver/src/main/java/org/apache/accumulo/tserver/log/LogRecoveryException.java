package org.apache.accumulo.tserver.log;

import static java.lang.String.format;

public class LogRecoveryException extends IllegalStateException {
  private String message;
  private String log;
  private int tabletId;

  public LogRecoveryException(String log, int tabletId, String message) {
    this.log = log;
    this.tabletId = tabletId;
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

  public int getTabletId() {
    return tabletId;
  }

  public String getLog() {
    return log;
  }

  @Override
  public String toString() {
    return format("Error processing log %s for tablet %s: %s", log, tabletId, message);
  }
}
