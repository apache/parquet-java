/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.io;

import java.io.IOException;

/**
 * {@code InputFile} is an interface with the methods needed by Parquet to read
 * data files using {@link SeekableInputStream} instances.
 */
public interface InputFile {

  /**
   * @return the total length of the file, in bytes.
   * @throws IOException if the length cannot be determined
   */
  long getLength() throws IOException;

  /**
   * Open a new {@link SeekableInputStream} for the underlying data file.
   *
   * @return a new {@link SeekableInputStream} to read the file
   * @throws IOException if the stream cannot be opened
   */
  SeekableInputStream newStream() throws IOException;
}
