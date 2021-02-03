package org.apache.accumulo.core.spi.fs;

import java.security.SecureRandom;
import java.util.Random;
import java.util.Set;

/**
 * @since 2.1.0
 */
public class RandomVolumeChooser implements VolumeChooser {
  protected final Random random = new SecureRandom();

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    String[] optionsArray = options.toArray(new String[0]);
    return optionsArray[random.nextInt(optionsArray.length)];
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    return options;
  }
}
