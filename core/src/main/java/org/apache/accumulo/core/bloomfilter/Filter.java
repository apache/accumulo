/*
 * Copyright (c) 2005, European Commission project OneLab under contract 034819
 * (http://www.one-lab.org)
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.apache.accumulo.core.bloomfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.HashFunction;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * Defines the general behavior of a filter.
 * <p>
 * A filter is a data structure which aims at offering a lossy summary of a set <code>A</code>. The
 * key idea is to map entries of <code>A</code> (also called <i>keys</i>) into several positions in
 * a vector through the use of several hash functions.
 * <p>
 * Typically, a filter will be implemented as a Bloom filter (or a Bloom filter extension).
 * <p>
 * It must be extended in order to define the real behavior.
 *
 * @see Key The general behavior of a key
 * @see HashFunction A hash function
 */
public abstract class Filter implements Writable {
  private static final int VERSION = -2; // negative to accommodate for old format
  /** The vector size of <i>this</i> filter. */
  protected int vectorSize;

  private int rVersion;
  /** The hash function used to map a key to several positions in the vector. */
  protected HashFunction hash;

  /** The number of hash function to consider. */
  protected int nbHash;

  /** Type of hashing function to use. */
  protected int hashType;

  protected Filter() {}

  /**
   * Constructor.
   *
   * @param vectorSize The vector size of <i>this</i> filter.
   * @param nbHash The number of hash functions to consider.
   * @param hashType type of the hashing function (see {@link Hash}).
   */
  protected Filter(final int vectorSize, final int nbHash, final int hashType) {
    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
    this.hashType = hashType;
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }

  /**
   * Adds a key to <i>this</i> filter.
   *
   * @param key The key to add.
   * @return true if the key was added, false otherwise.
   */
  public abstract boolean add(Key key);

  /**
   * Determines whether a specified key belongs to <i>this</i> filter.
   *
   * @param key The key to test.
   * @return boolean True if the specified key belongs to <i>this</i> filter. False otherwise.
   */
  public abstract boolean membershipTest(Key key);

  /**
   * Performs a logical AND between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to AND with.
   */
  public abstract void and(Filter filter);

  /**
   * Performs a logical OR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to OR with.
   */
  public abstract void or(Filter filter);

  /**
   * Performs a logical XOR between <i>this</i> filter and a specified filter.
   * <p>
   * <b>Invariant</b>: The result is assigned to <i>this</i> filter.
   *
   * @param filter The filter to XOR with.
   */
  public abstract void xor(Filter filter);

  /**
   * Performs a logical NOT on <i>this</i> filter.
   * <p>
   * The result is assigned to <i>this</i> filter.
   */
  public abstract void not();

  // Writable interface

  @Override
  public void write(final DataOutput out) throws IOException {
    out.writeInt(VERSION);
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeInt(this.vectorSize);
  }

  protected int getSerialVersion() {
    return rVersion;
  }

  protected int getVersion() {
    return VERSION;
  }

  @Override
  public void readFields(final DataInput in) throws IOException {
    final int ver = in.readInt();
    rVersion = ver;
    if (ver > 0) { // old unversioned format
      this.nbHash = ver;
      this.hashType = Hash.JENKINS_HASH;

    } else if (ver == VERSION | ver == VERSION + 1) { // Support for directly serializing the bitset
      this.nbHash = in.readInt();
      this.hashType = in.readByte();
    } else {
      throw new IOException("Unsupported version: " + ver);
    }
    this.vectorSize = in.readInt();
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }
}// end class
