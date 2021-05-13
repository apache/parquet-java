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
package org.apache.parquet.format.event;

import static java.util.Collections.unmodifiableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;

import org.apache.parquet.format.event.Consumers.Consumer;
import org.apache.parquet.format.event.TypedConsumer.ListConsumer;
import org.apache.parquet.format.event.TypedConsumer.StructConsumer;

/**
 * Entry point for reading thrift in a streaming fashion
 */
public class Consumers {

  /**
   * To consume objects coming from a DelegatingFieldConsumer
   *
   * @param <T> the type of consumed objects
   */
  public static interface Consumer<T> {
    void consume(T t);
  }

  /**
   * Delegates reading the field to TypedConsumers.
   * There is one TypedConsumer per thrift type.
   * use {@link #onField(TFieldIdEnum, TypedConsumer)} et al. to consume specific thrift fields.
   * @see Consumers#fieldConsumer()
   */
  public static class DelegatingFieldConsumer implements FieldConsumer {

    private final Map<Short, TypedConsumer> contexts;
    private final FieldConsumer defaultFieldEventConsumer;

    private DelegatingFieldConsumer(FieldConsumer defaultFieldEventConsumer, Map<Short, TypedConsumer> contexts) {
      this.defaultFieldEventConsumer = defaultFieldEventConsumer;
      this.contexts = unmodifiableMap(contexts);
    }

    private DelegatingFieldConsumer() {
      this(new SkippingFieldConsumer());
    }

    private DelegatingFieldConsumer(FieldConsumer defaultFieldEventConsumer) {
      this(defaultFieldEventConsumer, Collections.<Short, TypedConsumer>emptyMap());
    }

    public DelegatingFieldConsumer onField(TFieldIdEnum e, TypedConsumer typedConsumer) {
      Map<Short, TypedConsumer> newContexts = new HashMap<>(contexts);
      newContexts.put(e.getThriftFieldId(), typedConsumer);
      return new DelegatingFieldConsumer(defaultFieldEventConsumer, newContexts);
    }

    @Override
    public void consumeField(
        TProtocol protocol, EventBasedThriftReader reader,
        short id, byte type) throws TException {
      TypedConsumer delegate = contexts.get(id);
      if (delegate != null) {
        delegate.read(protocol, reader, type);
      } else {
        defaultFieldEventConsumer.consumeField(protocol, reader, id, type);
      }
    }
  }

  /**
   * call onField on the resulting DelegatingFieldConsumer to handle individual fields
   * @return a new DelegatingFieldConsumer
   */
  public static DelegatingFieldConsumer fieldConsumer() {
    return new DelegatingFieldConsumer();
  }

  /**
   * To consume a list of elements
   * @param c the class of the list content
   * @param consumer the consumer that will receive the list
   * @param <T> the type of the list content
   * @return a ListConsumer that can be passed to the DelegatingFieldConsumer
   */
  public static <T extends TBase<T,? extends TFieldIdEnum>> ListConsumer listOf(Class<T> c, final Consumer<List<T>> consumer) {
    class ListConsumer implements Consumer<T> {
      List<T> list;
      @Override
      public void consume(T t) {
        list.add(t);
      }
    }
    final ListConsumer co = new ListConsumer();
    return new DelegatingListElementsConsumer(struct(c, co)) {
      @Override
      public void consumeList(TProtocol protocol,
          EventBasedThriftReader reader, TList tList) throws TException {
        co.list = new ArrayList<>();
        super.consumeList(protocol, reader, tList);
        consumer.consume(co.list);
      }
    };
  }

  /**
   * To consume list elements one by one
   * @param consumer the consumer that will read the elements
   * @return a ListConsumer that can be passed to the DelegatingFieldConsumer
   */
  public static ListConsumer listElementsOf(TypedConsumer consumer) {
    return new DelegatingListElementsConsumer(consumer);
  }

  public static <T extends TBase<T,? extends TFieldIdEnum>> StructConsumer struct(final Class<T> c, final Consumer<T> consumer) {
    return new TBaseStructConsumer<>(c, consumer);
  }
}

class SkippingFieldConsumer implements FieldConsumer {
  @Override
  public void consumeField(TProtocol protocol, EventBasedThriftReader reader, short id, byte type) throws TException {
    TProtocolUtil.skip(protocol, type);
  }
}

class DelegatingListElementsConsumer extends ListConsumer {

  private TypedConsumer elementConsumer;

  protected DelegatingListElementsConsumer(TypedConsumer consumer) {
    this.elementConsumer = consumer;
  }

  @Override
  public void consumeElement(TProtocol protocol, EventBasedThriftReader reader, byte elemType) throws TException {
    elementConsumer.read(protocol, reader, elemType);
  }
}
class TBaseStructConsumer<T extends TBase<T, ? extends TFieldIdEnum>> extends StructConsumer {

  private final Class<T> c;
  private Consumer<T> consumer;

  public TBaseStructConsumer(Class<T> c, Consumer<T> consumer) {
    this.c = c;
    this.consumer = consumer;
  }

  @Override
  public void consumeStruct(TProtocol protocol, EventBasedThriftReader reader) throws TException {
    T o = newObject();
    o.read(protocol);
    consumer.consume(o);
  }

  protected T newObject() {
    try {
      return c.newInstance();
    } catch (InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(c.getName(), e);
    }
  }

}