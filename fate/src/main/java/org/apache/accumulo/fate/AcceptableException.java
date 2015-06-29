/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.fate;

/**
 * An exception for FATE operations to use to denote when an Exception is acceptable and should not trigger warning messages. This exception is intended to wrap
 * an existing exception from a FATE op implementation so that the FATE runner can know that the exception doesn't need to warn.
 * <p>
 * Often times, problems that map well into the FATE execution model have states in which it is impossible to know ahead of time if an exception will be thrown.
 * For example, with concurrent create table operations, one of the operations will fail because the table already exists, but this is not an error condition
 * for the system. It is normal and expected.
 */
public interface AcceptableException {

}
