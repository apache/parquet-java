package parquet.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

public class ParquetReadToWriteProtocol {

  public void readOne(TProtocol in, TProtocol out) throws TException {
    final TStruct struct = in.readStructBegin();
    out.writeStructBegin(struct);
    TField field;
    do {
      field = in.readFieldBegin();
      out.writeFieldBegin(field);
      readOneValue(in, out, field.type);
    } while (field.type != TType.STOP);
    in.readStructEnd();
    out.writeStructEnd();
  }

  private void readOneValue(TProtocol in, TProtocol out, byte type)
      throws TException {
    switch (type) {
    case TType.LIST:
      readOneList(in, out);
      break;
    case TType.MAP:
      readOneMap(in, out);
      break;
    case TType.SET:
      readOneSet(in, out);
      break;
    case TType.STRUCT:
      readOneStruct(in, out);
      break;
    case TType.STOP:
      break;
    case TType.BOOL:
      out.writeBool(in.readBool());
      break;
    case TType.BYTE:
      out.writeByte(in.readByte());
      break;
    case TType.DOUBLE:
      out.writeDouble(in.readDouble());
      break;
    case TType.I16:
      out.writeI16(in.readI16());
      break;
    case TType.ENUM: // same as i32 => actually never seen in the protocol layer as enums are written as a i32 field
    case TType.I32:
      out.writeI32(in.readI32());
      break;
    case TType.I64:
      out.writeI64(in.readI64());
      break;
    case TType.STRING:
      out.writeString(in.readString());
      break;
    case TType.VOID:
      break;
    default:
      throw new RuntimeException("Unknown type: " + type);
    }
  }

  private void readOneStruct(TProtocol in, TProtocol out) throws TException {
    final TStruct struct = in.readStructBegin();
    out.writeStructBegin(struct);
    TField field;
    do {
      field = in.readFieldBegin();
      out.writeFieldBegin(field);
      readOneValue(in, out, field.type);
      in.readFieldEnd();
      out.writeFieldEnd();
    } while (field.type != TType.STOP);
    in.readStructEnd();
    out.writeStructEnd();
  }

  private void readOneMap(TProtocol in, TProtocol out) throws TException {
    final TMap map = in.readMapBegin();
    out.writeMapBegin(map);
    for (int i = 0; i < map.size; i++) {
      readOneValue(in, out, map.keyType);
      readOneValue(in, out, map.valueType);
    }
    in.readMapEnd();
    out.writeMapEnd();
  }

  private void readOneSet(TProtocol in, TProtocol out) throws TException {
    final TSet set = in.readSetBegin();
    out.writeSetBegin(set);
    readCollectionElements(in, out, set.size, set.elemType);
    in.readSetEnd();
    out.writeSetEnd();
  }

  private void readOneList(TProtocol in, TProtocol out) throws TException {
    final TList list = in.readListBegin();
    out.writeListBegin(list);
    readCollectionElements(in, out, list.size, list.elemType);
    in.readListEnd();
    out.writeListEnd();
  }

  private void readCollectionElements(TProtocol in, TProtocol out,
      final int size, final byte elemType) throws TException {
    for (int i = 0; i < size; i++) {
      readOneValue(in, out, elemType);
    }
  }

}
