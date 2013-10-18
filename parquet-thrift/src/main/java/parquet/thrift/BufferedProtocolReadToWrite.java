/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.thrift;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import parquet.thrift.struct.ThriftField;
import parquet.thrift.struct.ThriftType;
import parquet.thrift.struct.ThriftType.ListType;
import parquet.thrift.struct.ThriftType.MapType;
import parquet.thrift.struct.ThriftType.SetType;
import parquet.thrift.struct.ThriftType.StructType;
import parquet.thrift.struct.ThriftTypeID;

/**
 * Class to read from one protocol in a buffer and then write to another one
 *
 * @author Julien Le Dem
 *
 */
public class BufferedProtocolReadToWrite implements ProtocolPipe {

  private interface Action {
    void write(TProtocol out) throws TException;
    String toDebugString();
  }

  private static final Action STRUCT_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeFieldStop();
      out.writeStructEnd();
    }

    @Override
    public String toDebugString() {
      return ")";
    }
  };

  private static final Action FIELD_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeFieldEnd();
    }
    @Override
    public String toDebugString() {
      return ";";
    }
  };

  private static final Action MAP_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeMapEnd();
    }
    @Override
    public String toDebugString() {
      return "]";
    }
  };

  private static final Action LIST_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeListEnd();
    }
    @Override
    public String toDebugString() {
      return "}";
    }
  };

  private static final Action SET_END = new Action() {
    @Override
    public void write(TProtocol out) throws TException {
      out.writeSetEnd();
    }
    @Override
    public String toDebugString() {
      return "*}";
    }
  };

  private final StructType thriftType;

  public BufferedProtocolReadToWrite(StructType thriftType) {
    super();
    this.thriftType = thriftType;
  }

  /**
   * reads one record from in and writes it to out
   * An Exception can be used to skip a bad record
   * @param in input protocol
   * @param out output protocol
   * @throws TException when an error happened while writing. Those are usualy not recoverable
   * @throws SkippableException when an error happened while reading. Ignoring those will skip the bad records.
   */
  @Override
  public void readOne(TProtocol in, TProtocol out) throws TException {
    List<Action> buffer = new LinkedList<Action>();
    try {
      readOneStruct(in, buffer, thriftType);
    } catch (Exception e) {
      throw new SkippableException(error("Error while reading", buffer), e);
    }
    try {
      for (Action a : buffer) {
        a.write(out);
      }
    } catch (Exception e) {
      throw new TException(error("Can not write record", buffer),e);
    }
  }

  private String error(String message, List<Action> buffer) {
    StringBuilder sb = new StringBuilder(message).append(": ");
    for (Action action : buffer) {
      sb.append(action.toDebugString());
    }
    return sb.toString();
  }

  private void readOneValue(TProtocol in, byte type, List<Action> buffer, ThriftType expectedType)
      throws TException {
    if (expectedType.getType().getSerializedThriftType() != type) {
      throw new TException("the data type does not match the expected thrift structure: expected " + expectedType + " got " + typeName(type));
    }
    switch (type) {
    case TType.LIST:
      readOneList(in, buffer, (ListType)expectedType);
      break;
    case TType.MAP:
      readOneMap(in, buffer, (MapType)expectedType);
      break;
    case TType.SET:
      readOneSet(in, buffer, (SetType)expectedType);
      break;
    case TType.STRUCT:
      readOneStruct(in, buffer, (StructType)expectedType);
      break;
    case TType.STOP:
      break;
    case TType.BOOL:
      final boolean bool = in.readBool();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeBool(bool);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(bool);
        }
      });
      break;
    case TType.BYTE:
      final byte b = in.readByte();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeByte(b);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(b);
        }
      });
      break;
    case TType.DOUBLE:
      final double d = in.readDouble();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeDouble(d);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(d);
        }
      });
      break;
    case TType.I16:
      final short s = in.readI16();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI16(s);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(s);
        }
      });
      break;
    case TType.ENUM: // same as i32 => actually never seen in the protocol layer as enums are written as a i32 field
    case TType.I32:
      final int i = in.readI32();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI32(i);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(i);
        }
      });
      break;
    case TType.I64:
      final long l = in.readI64();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeI64(l);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(l);
        }
      });
      break;
    case TType.STRING:
      final ByteBuffer bin = in.readBinary();
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeBinary(bin);
        }
        @Override
        public String toDebugString() {
          return String.valueOf(bin);
        }
      });
      break;
    case TType.VOID:
      break;
    default:
      throw new TException("Unknown type: " + type);
    }
  }

  private String typeName(byte type) {
    try {
      return ThriftTypeID.fromByte(type).name();
    } catch (RuntimeException e) {
      return String.valueOf(type);
    }
  }

  private void readOneStruct(TProtocol in, List<Action> buffer, StructType type) throws TException {
    final TStruct struct = in.readStructBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeStructBegin(struct);
      }
      @Override
      public String toDebugString() {
        return "(";
      }
    });
    TField field;
    while ((field = in.readFieldBegin()).type != TType.STOP) {
      final TField currentField = field;
      buffer.add(new Action() {
        @Override
        public void write(TProtocol out) throws TException {
          out.writeFieldBegin(currentField);
        }
        @Override
        public String toDebugString() {
          return "f=" + currentField.id + "<t=" + typeName(currentField.type) + ">: ";
        }
      });
      ThriftField expectedField = type.getChildById(field.id);
      try {
        readOneValue(in, field.type, buffer, expectedField.getType());
      } catch (Exception e) {
        throw new TException("Error while reading field " + field + " expected " + expectedField, e);
      }
      in.readFieldEnd();
      buffer.add(FIELD_END);
    }
    in.readStructEnd();
    buffer.add(STRUCT_END);
  }

  private void readOneMap(TProtocol in, List<Action> buffer, MapType mapType) throws TException {
    final TMap map = in.readMapBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeMapBegin(map);
      }
      @Override
      public String toDebugString() {
        return "<k=" + map.keyType + ", v=" + map.valueType + ", s=" + map.size + ">[";
      }
    });
    for (int i = 0; i < map.size; i++) {
      readOneValue(in, map.keyType, buffer, mapType.getKey().getType());
      readOneValue(in, map.valueType, buffer, mapType.getValue().getType());
    }
    in.readMapEnd();
    buffer.add(MAP_END);
  }

  private void readOneSet(TProtocol in, List<Action> buffer, SetType expectedType) throws TException {
    final TSet set = in.readSetBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeSetBegin(set);
      }
      @Override
      public String toDebugString() {
        return "<e=" + set.elemType + ", s=" + set.size + ">{*";
      }
    });
    readCollectionElements(in, set.size, set.elemType, buffer, expectedType.getValues().getType());
    in.readSetEnd();
    buffer.add(SET_END);
  }

  private void readOneList(TProtocol in, List<Action> buffer, ListType expectedType) throws TException {
    final TList list = in.readListBegin();
    buffer.add(new Action() {
      @Override
      public void write(TProtocol out) throws TException {
        out.writeListBegin(list);
      }
      @Override
      public String toDebugString() {
        return "<e=" + list.elemType + ", s=" + list.size + ">{";
      }
    });
    readCollectionElements(in, list.size, list.elemType, buffer, expectedType.getValues().getType());
    in.readListEnd();
    buffer.add(LIST_END);
  }

  private void readCollectionElements(TProtocol in,
      final int size, final byte elemType, List<Action> buffer, ThriftType expectedType) throws TException {
    for (int i = 0; i < size; i++) {
      readOneValue(in, elemType, buffer, expectedType);
    }
  }

}
