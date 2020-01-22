/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.cli.json;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.cli.util.RuntimeIOException;

public class AvroJsonReader<E> implements Iterator<E>, Iterable<E>, Closeable {

  private final GenericData model;
  private final Schema schema;
  private final InputStream stream;
  private Iterator<E> iterator;

  @SuppressWarnings("unchecked")
  public AvroJsonReader(InputStream stream, Schema schema) {
    this.stream = stream;
    this.schema = schema;
    this.model = GenericData.get();
    this.iterator = Iterators.transform(AvroJson.parser(stream),
        node -> (E) AvroJson.convertToAvro(model, node, AvroJsonReader.this.schema));
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public E next() {
    return iterator.next();
  }

  @Override
  public void close() {
    iterator = null;
    try {
      stream.close();
    } catch (IOException e) {
      throw new RuntimeIOException("Cannot close reader", e);
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Remove is not implemented.");
  }

  @Override
  public Iterator<E> iterator() {
    return this;
  }
}
