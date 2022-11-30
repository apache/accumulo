/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.Objects;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Partially based from {@link org.apache.commons.lang3.SerializationUtils}.
 *
 * <p>
 * For serializing and de-serializing objects.
 */
public class SerializationUtil {
  private static final Logger log = LoggerFactory.getLogger(SerializationUtil.class);

  private SerializationUtil() {}

  /**
   * Create a new instance of a class whose name is given, as a descendent of a given subclass.
   */
  public static <E> E subclassNewInstance(String classname, Class<E> parentClass) {
    Class<?> c;
    try {
      c = Class.forName(classname);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Can't find class: " + classname, e);
    }
    Class<? extends E> cm;
    try {
      cm = c.asSubclass(parentClass);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          classname + " is not a subclass of " + parentClass.getName(), e);
    }
    try {
      return cm.getDeclaredConstructor().newInstance();
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("can't instantiate new instance of " + cm.getName(), e);
    }
  }

  public static String serializeWritableBase64(Writable writable) {
    byte[] b = serializeWritable(writable);
    return Base64.getEncoder().encodeToString(b);
  }

  public static void deserializeWritableBase64(Writable writable, String str) {
    byte[] b = Base64.getDecoder().decode(str);
    deserializeWritable(writable, b);
  }

  public static String serializeBase64(Serializable obj) {
    byte[] b = serialize(obj);
    return Base64.getEncoder().encodeToString(b);
  }

  public static Object deserializeBase64(String str) {
    byte[] b = Base64.getDecoder().decode(str);
    return deserialize(b);
  }

  // Interop with Hadoop Writable
  // -----------------------------------------------------------------------

  public static byte[] serializeWritable(Writable writable) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    serializeWritable(writable, baos);
    return baos.toByteArray();
  }

  public static void serializeWritable(Writable obj, OutputStream outputStream) {
    Objects.requireNonNull(obj);
    Objects.requireNonNull(outputStream);
    DataOutputStream out = null;
    try {
      out = new DataOutputStream(outputStream);
      obj.write(out);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          log.error("cannot close", e);
        }
      }
    }
  }

  public static void deserializeWritable(Writable writable, InputStream inputStream) {
    Objects.requireNonNull(writable);
    Objects.requireNonNull(inputStream);
    DataInputStream in = null;
    try {
      in = new DataInputStream(inputStream);
      writable.readFields(in);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.error("cannot close", e);
        }
      }
    }
  }

  public static void deserializeWritable(Writable writable, byte[] objectData) {
    Objects.requireNonNull(objectData);
    deserializeWritable(writable, new ByteArrayInputStream(objectData));
  }

  // Serialize
  // -----------------------------------------------------------------------

  /**
   * Serializes an {@code Object} to the specified stream.
   * <p>
   * The stream will be closed once the object is written. This avoids the need for a finally
   * clause, and maybe also exception handling, in the application code.
   * <p>
   * The stream passed in is not buffered internally within this method. This is the responsibility
   * of your application if desired.
   *
   * @param obj the object to serialize to bytes, may be null
   * @param outputStream the stream to write to, must not be null
   * @throws NullPointerException if {@code outputStream} is {@code null}
   */
  public static void serialize(Serializable obj, OutputStream outputStream) {
    Objects.requireNonNull(outputStream);
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(outputStream);
      out.writeObject(obj);
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          log.error("cannot close", e);
        }
      }
    }
  }

  /**
   * Serializes an {@code Object} to a byte array for storage/serialization.
   *
   * @param obj the object to serialize to bytes
   * @return a byte[] with the converted Serializable
   */
  public static byte[] serialize(Serializable obj) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
    serialize(obj, baos);
    return baos.toByteArray();
  }

  // Deserialize
  // -----------------------------------------------------------------------

  /**
   * Deserializes an {@code Object} from the specified stream.
   * <p>
   * The stream will be closed once the object is written. This avoids the need for a finally
   * clause, and maybe also exception handling, in the application code.
   * <p>
   * The stream passed in is not buffered internally within this method. This is the responsibility
   * of your application if desired.
   *
   * @param inputStream the serialized object input stream, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code inputStream} is {@code null}
   */
  @SuppressFBWarnings(value = "OBJECT_DESERIALIZATION", justification = "okay for test")
  public static Object deserialize(InputStream inputStream) {
    Objects.requireNonNull(inputStream);
    ObjectInputStream in = null;
    try {
      in = new ObjectInputStream(inputStream);
      return in.readObject();
    } catch (ClassNotFoundException | IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          log.error("cannot close", e);
        }
      }
    }
  }

  /**
   * Deserializes a single {@code Object} from an array of bytes.
   *
   * @param objectData the serialized object, must not be null
   * @return the deserialized object
   * @throws IllegalArgumentException if {@code objectData} is {@code null}
   */
  public static Object deserialize(byte[] objectData) {
    Objects.requireNonNull(objectData);
    return deserialize(new ByteArrayInputStream(objectData));
  }

}
