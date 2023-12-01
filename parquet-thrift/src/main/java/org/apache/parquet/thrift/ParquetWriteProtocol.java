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
package org.apache.parquet.thrift;

import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.thrift.struct.ThriftField;
import org.apache.parquet.thrift.struct.ThriftType;
import org.apache.parquet.thrift.struct.ThriftType.EnumType;
import org.apache.parquet.thrift.struct.ThriftType.EnumValue;
import org.apache.parquet.thrift.struct.ThriftType.ListType;
import org.apache.parquet.thrift.struct.ThriftType.MapType;
import org.apache.parquet.thrift.struct.ThriftType.SetType;
import org.apache.parquet.thrift.struct.ThriftType.StructType;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetWriteProtocol extends ParquetProtocol {

  public static final String WRITE_THREE_LEVEL_LISTS = "parquet.thrift.write-three-level-lists";
  public static final boolean WRITE_THREE_LEVEL_LISTS_DEFAULT = false;

  private boolean writeThreeLevelList = WRITE_THREE_LEVEL_LISTS_DEFAULT;

  interface Events {

    public void start();

    public void end();
  }

  abstract class FieldBaseWriteProtocol extends ParquetProtocol {
    private final Events returnClause;

    public FieldBaseWriteProtocol(Events returnClause) {
      this.returnClause = returnClause;
    }

    void start() {
      this.returnClause.start();
    }

    void end() {
      this.returnClause.end();
    }
  }

  class EnumWriteProtocol extends FieldBaseWriteProtocol {

    private final EnumType type;
    private PrimitiveColumnIO columnIO;

    public EnumWriteProtocol(PrimitiveColumnIO columnIO, EnumType type, Events returnClause) {
      super(returnClause);
      this.columnIO = columnIO;
      this.type = type;
    }

    @Override
    public void writeI32(int i32) throws TException {
      start();
      EnumValue value = type.getEnumValueById(i32);
      if (value == null) {
        throw new ParquetEncodingException(
            "Can not find enum value of index " + i32 + " for field:" + columnIO.toString());
      }
      recordConsumer.addBinary(Binary.fromString(value.getName()));
      end();
    }
  }

  abstract class ListWriteProtocol extends FieldBaseWriteProtocol {
    protected int size;

    public ListWriteProtocol(Events returnClause) {
      super(returnClause);
    }

    protected abstract void startListWrapper();

    protected abstract void endListWrapper();

    @Override
    public void writeListBegin(TList list) throws TException {
      size = list.size;
      startListWrapper();
    }

    @Override
    public void writeListEnd() throws TException {
      endListWrapper();
    }

    @Override
    public void writeSetBegin(TSet set) throws TException {
      size = set.size;
      startListWrapper();
    }

    @Override
    public void writeSetEnd() throws TException {
      endListWrapper();
    }
  }

  class TwoLevelListWriteProtocol extends ListWriteProtocol {
    private ColumnIO listContent;
    private TProtocol contentProtocol;

    public TwoLevelListWriteProtocol(GroupColumnIO columnIO, ThriftField values, Events returnClause) {
      super(returnClause);
      this.listContent = columnIO.getChild(0);
      this.contentProtocol = getProtocol(values, listContent, new Events() {
        int consumedRecords = 0;

        @Override
        public void start() {}

        @Override
        public void end() {
          ++consumedRecords;
          if (consumedRecords == size) {
            currentProtocol = TwoLevelListWriteProtocol.this;
            consumedRecords = 0;
          }
        }
      });
    }

    @Override
    protected void startListWrapper() {
      start();
      recordConsumer.startGroup();
      if (size > 0) {
        recordConsumer.startField(listContent.getType().getName(), 0);
        currentProtocol = contentProtocol;
      }
    }

    @Override
    protected void endListWrapper() {
      if (size > 0) {
        recordConsumer.endField(listContent.getType().getName(), 0);
      }
      recordConsumer.endGroup();
      end();
    }
  }

  class ThreeLevelListWriteProtocol extends ListWriteProtocol {
    private GroupColumnIO listContent;
    private ColumnIO elementContent;
    private TProtocol contentProtocol;

    public ThreeLevelListWriteProtocol(GroupColumnIO columnIO, ThriftField values, Events returnClause) {
      super(returnClause);
      this.listContent = (GroupColumnIO) columnIO.getChild(0);
      this.elementContent = listContent.getChild(0);
      this.contentProtocol = getProtocol(values, elementContent, new Events() {
        int consumedRecords = 0;

        @Override
        public void start() {
          recordConsumer.startGroup();
          recordConsumer.startField("element", 0);
        }

        @Override
        public void end() {
          recordConsumer.endField("element", 0);
          recordConsumer.endGroup();
          ++consumedRecords;
          if (consumedRecords == size) {
            currentProtocol = ThreeLevelListWriteProtocol.this;
            consumedRecords = 0;
          } else {
            currentProtocol = contentProtocol;
          }
        }
      });
    }

    @Override
    protected void startListWrapper() {
      start();
      recordConsumer.startGroup();
      if (size > 0) {
        recordConsumer.startField("list", 0);
        currentProtocol = contentProtocol;
      }
    }

    @Override
    protected void endListWrapper() {
      if (size > 0) {
        recordConsumer.endField("list", 0);
      }
      recordConsumer.endGroup();
      end();
    }
  }

  class MapWriteProtocol extends FieldBaseWriteProtocol {

    private GroupColumnIO mapContent;
    private ColumnIO key;
    private ColumnIO value;
    private TProtocol keyProtocol;
    private TProtocol valueProtocol;
    private int countToConsume;

    public MapWriteProtocol(GroupColumnIO columnIO, MapType type, Events returnClause) {
      super(returnClause);
      this.mapContent = (GroupColumnIO) columnIO.getChild(0);
      this.key = mapContent.getChild(0);
      this.value = mapContent.getChild(1);
      this.keyProtocol = getProtocol(type.getKey(), this.key, new Events() {
        @Override
        public void start() {
          recordConsumer.startGroup();
          recordConsumer.startField(key.getName(), key.getIndex());
        }

        @Override
        public void end() {
          recordConsumer.endField(key.getName(), key.getIndex());
          currentProtocol = valueProtocol;
        }
      });
      this.valueProtocol = getProtocol(type.getValue(), this.value, new Events() {
        int consumed;

        @Override
        public void start() {
          recordConsumer.startField(value.getName(), value.getIndex());
        }

        @Override
        public void end() {
          consumed++;
          recordConsumer.endField(value.getName(), value.getIndex());
          recordConsumer.endGroup();
          if (consumed == countToConsume) {
            currentProtocol = MapWriteProtocol.this;
            consumed = 0;
          } else {
            currentProtocol = keyProtocol;
          }
        }
      });
    }

    @Override
    public void writeMapBegin(TMap map) throws TException {
      start();
      recordConsumer.startGroup();
      countToConsume = map.size;
      if (countToConsume > 0) {
        recordConsumer.startField(mapContent.getType().getName(), 0);
        currentProtocol = keyProtocol;
      }
    }

    @Override
    public void writeMapEnd() throws TException {
      if (countToConsume > 0) {
        recordConsumer.endField(mapContent.getType().getName(), 0);
      }
      recordConsumer.endGroup();
      end();
    }
  }

  class PrimitiveWriteProtocol extends FieldBaseWriteProtocol {

    public PrimitiveWriteProtocol(PrimitiveColumnIO columnIO, Events returnClause) {
      super(returnClause);
    }

    @Override
    public void writeBool(boolean b) throws TException {
      start();
      recordConsumer.addBoolean(b);
      end();
    }

    @Override
    public void writeByte(byte b) throws TException {
      start();
      recordConsumer.addInteger(b);
      end();
    }

    @Override
    public void writeI16(short i16) throws TException {
      start();
      recordConsumer.addInteger(i16);
      end();
    }

    @Override
    public void writeI32(int i32) throws TException {
      start();
      recordConsumer.addInteger(i32);
      end();
    }

    @Override
    public void writeI64(long i64) throws TException {
      start();
      recordConsumer.addLong(i64);
      end();
    }

    @Override
    public void writeDouble(double dub) throws TException {
      start();
      recordConsumer.addDouble(dub);
      end();
    }

    @Override
    public void writeString(String str) throws TException {
      start();
      writeStringToRecordConsumer(str);
      end();
    }

    @Override
    public void writeBinary(ByteBuffer buf) throws TException {
      start();
      writeBinaryToRecordConsumer(buf);
      end();
    }
  }

  class StructWriteProtocol extends FieldBaseWriteProtocol {

    private final GroupColumnIO schema;
    private final StructType thriftType;
    private final TProtocol[] children;
    private ColumnIO currentType;
    private ColumnIO[] thriftFieldIdToParquetField;

    public StructWriteProtocol(GroupColumnIO schema, StructType thriftType, Events returnClause) {
      super(returnClause);
      if (schema == null) {
        throw new NullPointerException("schema");
      }
      this.thriftType = thriftType;
      int maxFieldId = 0;
      for (ThriftField field : thriftType.getChildren()) {
        maxFieldId = Math.max(maxFieldId, field.getFieldId());
      }
      thriftFieldIdToParquetField = new ColumnIO[maxFieldId + 1];
      for (int i = 0; i < thriftType.getChildren().size(); i++) {
        thriftFieldIdToParquetField[thriftType.getChildren().get(i).getFieldId()] = schema.getChild(i);
      }
      for (ThriftField field : thriftType.getChildren()) {}
      this.schema = schema;
      children = new TProtocol[thriftType.getChildren().size()];
      for (int i = 0; i < children.length; i++) {
        final ThriftField field = thriftType.getChildren().get(i);
        final ColumnIO columnIO = schema.getChild(field.getName());
        if (columnIO == null) {
          throw new RuntimeException("Could not find " + field.getName() + " in " + schema);
        }
        try {
          TProtocol p;
          p = getProtocol(field, columnIO, new Events() {
            @Override
            public void start() {}

            @Override
            public void end() {
              currentProtocol = StructWriteProtocol.this;
            }
          });
          children[i] = p;
        } catch (RuntimeException e) {
          throw new ParquetEncodingException("Could not create Protocol for " + field + " to " + columnIO, e);
        }
      }
    }

    @Override
    public void writeStructBegin(TStruct struct) throws TException {
      start();
      recordConsumer.startGroup();
    }

    @Override
    public void writeStructEnd() throws TException {
      recordConsumer.endGroup();
      end();
    }

    @Override
    public void writeFieldBegin(TField field) throws TException {
      if (field.type == TType.STOP) {
        return;
      }
      try {
        currentType = thriftFieldIdToParquetField[field.id];
        if (currentType == null) {
          throw new ParquetEncodingException(
              "field " + field.id + " was not found in " + thriftType + " and " + schema.getType());
        }
        final int index = currentType.getIndex();
        recordConsumer.startField(currentType.getName(), index);
        currentProtocol = children[index];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new ParquetEncodingException(
            "field " + field.id + " was not found in " + thriftType + " and " + schema.getType());
      }
    }

    @Override
    public void writeFieldStop() throws TException {
      // duplicate with struct end
    }

    @Override
    public void writeFieldEnd() throws TException {
      recordConsumer.endField(currentType.getName(), currentType.getIndex());
    }
  }

  class MessageWriteProtocol extends StructWriteProtocol {

    public MessageWriteProtocol(MessageColumnIO schema, StructType thriftType) {
      super(schema, thriftType, null);
    }

    @Override
    public void writeStructBegin(TStruct struct) throws TException {
      recordConsumer.startMessage();
    }

    @Override
    public void writeStructEnd() throws TException {
      recordConsumer.endMessage();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ParquetWriteProtocol.class);

  private final RecordConsumer recordConsumer;
  private TProtocol currentProtocol;

  private String toString(TStruct struct) {
    return "<TStruct name:" + struct.name + ">";
  }

  private String toString(TList list) {
    return "<TList elemType:" + list.elemType + " size:" + list.size + ">";
  }

  private String toString(TMap map) {
    return "<TMap keyType:" + map.keyType + " valueType:" + map.valueType + " size:" + map.size + ">";
  }

  public ParquetWriteProtocol(
      Configuration configuration, RecordConsumer recordConsumer, MessageColumnIO schema, StructType thriftType) {
    this(new HadoopParquetConfiguration(configuration), recordConsumer, schema, thriftType);
  }

  public ParquetWriteProtocol(
      ParquetConfiguration configuration,
      RecordConsumer recordConsumer,
      MessageColumnIO schema,
      StructType thriftType) {
    this.recordConsumer = recordConsumer;
    if (configuration != null) {
      this.writeThreeLevelList =
          configuration.getBoolean(WRITE_THREE_LEVEL_LISTS, WRITE_THREE_LEVEL_LISTS_DEFAULT);
    }
    this.currentProtocol = new MessageWriteProtocol(schema, thriftType);
  }

  public ParquetWriteProtocol(RecordConsumer recordConsumer, MessageColumnIO schema, StructType thriftType) {
    this.currentProtocol = new MessageWriteProtocol(schema, thriftType);
    this.recordConsumer = recordConsumer;
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeMessageBegin(org.apache.thrift.protocol.TMessage)
   */
  @Override
  public void writeMessageBegin(TMessage message) throws TException {
    LOG.debug("writeMessageBegin({})", message);
    currentProtocol.writeMessageBegin(message);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeMessageEnd()
   */
  @Override
  public void writeMessageEnd() throws TException {
    LOG.debug("writeMessageEnd()");
    currentProtocol.writeMessageEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeStructBegin(org.apache.thrift.protocol.TStruct)
   */
  @Override
  public void writeStructBegin(TStruct struct) throws TException {
    if (LOG.isDebugEnabled()) LOG.debug("writeStructBegin(" + toString(struct) + ")");
    currentProtocol.writeStructBegin(struct);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeStructEnd()
   */
  @Override
  public void writeStructEnd() throws TException {
    LOG.debug("writeStructEnd()");
    currentProtocol.writeStructEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeFieldBegin(org.apache.thrift.protocol.TField)
   */
  @Override
  public void writeFieldBegin(TField field) throws TException {
    LOG.debug("writeFieldBegin({})", field);
    currentProtocol.writeFieldBegin(field);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeFieldEnd()
   */
  @Override
  public void writeFieldEnd() throws TException {
    LOG.debug("writeFieldEnd()");
    currentProtocol.writeFieldEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeFieldStop()
   */
  @Override
  public void writeFieldStop() throws TException {
    LOG.debug("writeFieldStop()");
    currentProtocol.writeFieldStop();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeMapBegin(org.apache.thrift.protocol.TMap)
   */
  @Override
  public void writeMapBegin(TMap map) throws TException {
    if (LOG.isDebugEnabled()) LOG.debug("writeMapBegin(" + toString(map) + ")");
    currentProtocol.writeMapBegin(map);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeMapEnd()
   */
  @Override
  public void writeMapEnd() throws TException {
    LOG.debug("writeMapEnd()");
    currentProtocol.writeMapEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeListBegin(org.apache.thrift.protocol.TList)
   */
  @Override
  public void writeListBegin(TList list) throws TException {
    if (LOG.isDebugEnabled()) LOG.debug("writeListBegin(" + toString(list) + ")");
    currentProtocol.writeListBegin(list);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeListEnd()
   */
  @Override
  public void writeListEnd() throws TException {
    LOG.debug("writeListEnd()");
    currentProtocol.writeListEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeSetBegin(org.apache.thrift.protocol.TSet)
   */
  @Override
  public void writeSetBegin(TSet set) throws TException {
    LOG.debug("writeSetBegin({})", set);
    currentProtocol.writeSetBegin(set);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeSetEnd()
   */
  @Override
  public void writeSetEnd() throws TException {
    LOG.debug("writeSetEnd()");
    currentProtocol.writeSetEnd();
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeBool(boolean)
   */
  @Override
  public void writeBool(boolean b) throws TException {
    LOG.debug("writeBool({})", b);
    currentProtocol.writeBool(b);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeByte(byte)
   */
  @Override
  public void writeByte(byte b) throws TException {
    LOG.debug("writeByte({})", b);
    currentProtocol.writeByte(b);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeI16(short)
   */
  @Override
  public void writeI16(short i16) throws TException {
    LOG.debug("writeI16({})", i16);
    currentProtocol.writeI16(i16);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeI32(int)
   */
  @Override
  public void writeI32(int i32) throws TException {
    LOG.debug("writeI32({})", i32);
    currentProtocol.writeI32(i32);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeI64(long)
   */
  @Override
  public void writeI64(long i64) throws TException {
    LOG.debug("writeI64({})", i64);
    currentProtocol.writeI64(i64);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeDouble(double)
   */
  @Override
  public void writeDouble(double dub) throws TException {
    LOG.debug("writeDouble({})", dub);
    currentProtocol.writeDouble(dub);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeString(java.lang.String)
   */
  @Override
  public void writeString(String str) throws TException {
    LOG.debug("writeString({})", str);
    currentProtocol.writeString(str);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.apache.parquet.thrift.ParquetProtocol#writeBinary(java.nio.ByteBuffer)
   */
  @Override
  public void writeBinary(ByteBuffer buf) throws TException {
    LOG.debug("writeBinary({})", buf);
    currentProtocol.writeBinary(buf);
  }

  private void writeBinaryToRecordConsumer(ByteBuffer buf) {
    recordConsumer.addBinary(Binary.fromReusedByteArray(buf.array(), buf.position(), buf.limit() - buf.position()));
  }

  private void writeStringToRecordConsumer(String str) {
    recordConsumer.addBinary(Binary.fromString(str));
  }

  private TProtocol getProtocol(ThriftField field, ColumnIO columnIO, Events returnClause) {
    TProtocol p;
    final ThriftType type = field.getType();
    switch (type.getType()) {
      case STOP:
      case VOID:
      default:
        throw new UnsupportedOperationException("can't convert type of " + field);
      case BOOL:
      case BYTE:
      case DOUBLE:
      case I16:
      case I32:
      case I64:
      case STRING:
      case UUID:
        p = new PrimitiveWriteProtocol((PrimitiveColumnIO) columnIO, returnClause);
        break;
      case STRUCT:
        p = new StructWriteProtocol((GroupColumnIO) columnIO, (StructType) type, returnClause);
        break;
      case MAP:
        p = new MapWriteProtocol((GroupColumnIO) columnIO, (MapType) type, returnClause);
        break;
      case SET:
        p = getListWriteProtocol((GroupColumnIO) columnIO, ((SetType) type).getValues(), returnClause);
        break;
      case LIST:
        p = getListWriteProtocol((GroupColumnIO) columnIO, ((ListType) type).getValues(), returnClause);
        break;
      case ENUM:
        p = new EnumWriteProtocol((PrimitiveColumnIO) columnIO, (EnumType) type, returnClause);
        break;
    }
    return p;
  }

  private TProtocol getListWriteProtocol(GroupColumnIO columnIO, ThriftField values, Events returnClause) {
    return writeThreeLevelList
        ? new ThreeLevelListWriteProtocol(columnIO, values, returnClause)
        : new TwoLevelListWriteProtocol(columnIO, values, returnClause);
  }
}
