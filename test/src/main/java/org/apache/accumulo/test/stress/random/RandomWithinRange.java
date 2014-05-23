package org.apache.accumulo.test.stress.random;

import java.util.Random;

import com.google.common.base.Preconditions;

/**
 * Class that returns positive integers between some minimum
 * and maximum.
 *
 */
public class RandomWithinRange {
  private final Random random;
  private final int min, max;
  
  public RandomWithinRange(int seed, int min, int max) {
    this(new Random(seed), min, max);
  }
  
  public RandomWithinRange(Random random, int min, int max) {
    Preconditions.checkArgument(min > 0, "Min must be positive.");
    Preconditions.checkArgument(max >= min, "Max must be greater than or equal to min.");
    this.random = random;
    this.min = min;
    this.max = max;
  }
  
  public int next() {
    if (min == max) {
      return min;
    } else {
      // we pick a random number that's between 0 and (max - min), then add
      // min as an offset to get a random number that's [min, max)
      return random.nextInt(max - min) + min;
    }
  }
  
  public byte[] next_bytes() {
    byte[] b = new byte[next()];
    random.nextBytes(b);
    return b;
  }
}
