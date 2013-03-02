package parquet.hadoop.thrift;

import static junit.framework.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;

import parquet.column.mem.MemColumnWriteStore;
import parquet.column.mem.MemPageStore;
import parquet.io.ColumnIOFactory;
import parquet.io.MessageColumnIO;
import parquet.io.RecordConsumer;
import parquet.io.RecordReader;
import parquet.schema.MessageType;
import parquet.thrift.ParquetWriteProtocol;
import parquet.thrift.ThriftReader;
import parquet.thrift.ThriftRecordConverter;
import parquet.thrift.ThriftSchemaConverter;
import parquet.thrift.struct.ThriftType.StructType;

import com.twitter.data.proto.tutorial.thrift.AddressBook;
import com.twitter.data.proto.tutorial.thrift.Name;
import com.twitter.data.proto.tutorial.thrift.Person;
import com.twitter.data.proto.tutorial.thrift.PhoneNumber;

public class TestParquetReadProtocol {

  @Test
  public void testReadEmpty() throws Exception {
    AddressBook expected = new AddressBook();
    validate(expected);
  }

  @Test
  public void testRead() throws Exception {
    List<Person> persons = Arrays.asList(
        new Person(
            new Name("john", "johson"),
            1,
            "john@johnson.org",
            Arrays.asList(new PhoneNumber("5555555555"))
            ),
        new Person(
            new Name("jack", "jackson"),
            2,
            "jack@jackson.org",
            Arrays.asList(new PhoneNumber("5555555556"))
            )
        );
    AddressBook expected = new AddressBook(persons);
    validate(expected);
  }

  private void validate(TBase<?,?> expected) throws TException {
    @SuppressWarnings("unchecked")
    final Class<? extends TBase<?,?>> thriftClass = (Class<? extends TBase<?,?>>)expected.getClass();
    final MemPageStore memPageStore = new MemPageStore();
    final ThriftSchemaConverter schemaConverter = new ThriftSchemaConverter();
    final MessageType schema = schemaConverter.convert(thriftClass);
    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
    final MemColumnWriteStore columns = new MemColumnWriteStore(memPageStore, 10000);
    final RecordConsumer recordWriter = columnIO.getRecordWriter(columns);
    final StructType thriftType = schemaConverter.toStructType(thriftClass);
    ParquetWriteProtocol parquetWriteProtocol = new ParquetWriteProtocol(recordWriter, columnIO, thriftType);

    expected.write(parquetWriteProtocol);
    columns.flush();

    ThriftRecordConverter<AddressBook> converter = new ThriftRecordConverter<AddressBook>(new ThriftReader<AddressBook>() {
      @Override
      public AddressBook readOneRecord(TProtocol protocol) throws TException {
        AddressBook a = new AddressBook();
        a.read(protocol);
        return a;
      }

    }, thriftClass.getSimpleName(), schema, thriftType);

    final RecordReader<AddressBook> recordReader = columnIO.getRecordReader(memPageStore, converter);

    final AddressBook result = recordReader.read();

    assertEquals(expected, result);
  }

}
