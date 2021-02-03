package org.apache.accumulo.core.spi.fs;

import java.util.Set;

import org.apache.accumulo.core.conf.Property;

/**
 * Helper used to select from a set of Volume URIs. N.B. implementations must be threadsafe.
 * VolumeChooser.equals will be used for internal caching.
 *
 * <p>
 * Implementations may wish to store configuration in Accumulo's system configuration using the
 * {@link Property#GENERAL_ARBITRARY_PROP_PREFIX}. They may also benefit from using per-table
 * configuration using {@link Property#TABLE_ARBITRARY_PROP_PREFIX}.
 *
 * @since 2.1.0
 */
public interface VolumeChooser {

  /**
   * Choose a volume from the provided options.
   *
   * @param env
   *          the server environment provided by the calling framework
   * @param options
   *          the list of volumes to choose from
   * @return one of the options
   * @throws VolumeChooserException
   *           if there is an error choosing (this is a RuntimeException); this does not preclude
   *           other RuntimeExceptions from occurring
   */
  String choose(VolumeChooserEnvironment env, Set<String> options) throws VolumeChooserException;

  /**
   * Return the subset of volumes that could possibly be chosen by this chooser across all
   * invocations of {@link #choose(VolumeChooserEnvironment, Set)}.
   *
   * @param env
   *          the server environment provided by the calling framework
   * @param options
   *          the subset of volumes to choose from
   * @return array of valid options
   * @throws VolumeChooserException
   *           if there is an error choosing (this is a RuntimeException); this does not preclude
   *           other RuntimeExceptions from occurring
   *
   */
  Set<String> choosable(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException;

  class VolumeChooserException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public VolumeChooserException(String message) {
      super(message);
    }

    public VolumeChooserException(String message, Throwable cause) {
      super(message, cause);
    }

  }

}
