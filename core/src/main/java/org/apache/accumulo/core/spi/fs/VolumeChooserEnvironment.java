package org.apache.accumulo.core.spi.fs;

import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.hadoop.io.Text;

/**
 * @since 2.1.0
 */
public interface VolumeChooserEnvironment {
  /**
   * A scope the volume chooser environment; a TABLE scope should be accompanied by a tableId.
   *
   * @since 2.1.0
   */
  public static enum Scope {
    DEFAULT, TABLE, INIT, LOGGER
  }

  public Text getEndRow();

  public boolean hasTableId();

  public TableId getTableId();

  public Scope getChooserScope();

  public ServiceEnvironment getServiceEnv();
}
