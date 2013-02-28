package parquet.hadoop.example;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import parquet.example.data.Group;
import parquet.example.data.simple.convert.GroupRecordConverter;
import parquet.hadoop.ReadSupport;
import parquet.io.convert.RecordConverter;
import parquet.schema.MessageType;

public class GroupReadSupport extends ReadSupport<Group> {

  @Override
  public RecordConverter<Group> initForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fielSchema,
      MessageType requestedSchema) {
    return new GroupRecordConverter(requestedSchema);
  }

}
