package parquet.format.event;

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

import parquet.format.event.Consumers.Consumer;
import parquet.format.event.TypedConsumer.BoolConsumer;
import parquet.format.event.TypedConsumer.ListConsumer;
import parquet.format.event.TypedConsumer.StructConsumer;

/**
 * Entry point for reading thrift in a streaming fashion
 *
 * @author Julien Le Dem
 *
 */
public class Consumers {

  /**
   * To consume objects coming from a DelegatingFieldConsumer
   * @author Julien Le Dem
   *
   * @param <T> the type of consumed objects
   */
  public static interface Consumer<T> {
    void consume(T t);
  }

  /**
   * Delegates reading the field to TypedConsumers.
   * There is one TypedConsumer per thrift type.
   * use {@link DelegatingFieldConsumer#onField(TFieldIdEnum, BoolConsumer)} et al. to consume specific thrift fields.
   * @see Consumers#fieldConsumer()
   * @author Julien Le Dem
   *
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
      Map<Short, TypedConsumer> newContexts = new HashMap<Short, TypedConsumer>(contexts);
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
   * @param c the type of the list content
   * @param consumer the consumer that will receive the list
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
        co.list = new ArrayList<T>();
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
    return new TBaseStructConsumer<T>(c, consumer);
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
    } catch (InstantiationException e) {
      throw new RuntimeException(c.getName(), e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(c.getName(), e);
    }
  }

}