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
package org.apache.parquet.benchmarks;

import static java.util.UUID.randomUUID;
import static org.apache.parquet.benchmarks.BenchmarkConstants.DICT_PAGE_SIZE;
import static org.apache.parquet.benchmarks.BenchmarkUtils.exists;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.benchmarks.Messages.Test1;
import org.apache.parquet.benchmarks.Messages.Test100Int32;
import org.apache.parquet.benchmarks.Messages.Test30Int32;
import org.apache.parquet.benchmarks.Messages.Test30String;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.ProtoWriteSupport;

public class ProtoDataGenerator<B extends Message.Builder, M extends Message> extends DataGenerator {

  private final Class<M> protoClass;
  private final ProtoWriteSupport.CodegenMode codegenMode;
  private final RecordGeneratorFactory<B> recordGeneratorFactory;

  public ProtoDataGenerator(Class<M> protoClass, ProtoWriteSupport.CodegenMode codegenMode) {
    this.protoClass = protoClass;
    this.codegenMode = codegenMode;
    this.recordGeneratorFactory = (RecordGeneratorFactory<B>) GENERATORS.get(protoClass);
  }

  private interface RecordGeneratorFactory<B extends Message.Builder> {
    RecordGenerator<B> newRecordGenerator(int fixedLenByteArraySize);
  }

  private interface RecordGenerator<B extends Message.Builder> extends IntFunction<B> {}

  private static final RecordGeneratorFactory<Test1.Builder> TEST1 = fixedLenByteArraySize -> {
    final Test1.Builder builder = Test1.newBuilder();
    String fixedLenStr = generateFixedLenStr(fixedLenByteArraySize);

    return i -> builder.setBinaryField(ByteString.copyFromUtf8(randomUUID().toString()))
        .setInt32Field(i)
        .setInt64Field(64L)
        .setBooleanField(true)
        .setFloatField(1.0f)
        .setDoubleField(2.0d)
        .setStringField(fixedLenStr);
  };

  private static final RecordGeneratorFactory<Test30Int32.Builder> TEST_30_INT32 = fixedLenByteArraySize -> {
    final Test30Int32.Builder builder = Test30Int32.newBuilder();

    return i -> builder
        .setField1(i)
        .setField2(i)
        .setField3(i)
        .setField4(i)
        .setField5(i)
        .setField6(i)
        .setField7(i)
        .setField8(i)
        .setField9(i)
        .setField10(i)
        .setField11(i)
        .setField12(i)
        .setField13(i)
        .setField14(i)
        .setField15(i)
        .setField16(i)
        .setField17(i)
        .setField18(i)
        .setField19(i)
        .setField20(i)
        .setField21(i)
        .setField22(i)
        .setField23(i)
        .setField24(i)
        .setField25(i)
        .setField26(i)
        .setField27(i)
        .setField28(i)
        .setField29(i)
        .setField30(i);
  };

  private static final RecordGeneratorFactory<Test100Int32.Builder> TEST_100_INT32 = fixedLenByteArraySize -> {
    final Test100Int32.Builder builder = Test100Int32.newBuilder();

    return i -> builder
        .setF1(i).setF2(i).setF3(i).setF4(i).setF5(i).setF6(i).setF7(i).setF8(i).setF9(i).setF10(i)
        .setF11(i).setF12(i).setF13(i).setF14(i).setF15(i).setF16(i).setF17(i).setF18(i).setF19(i).setF20(i)
        .setF21(i).setF22(i).setF23(i).setF24(i).setF25(i).setF26(i).setF27(i).setF28(i).setF29(i).setF30(i)
        .setF31(i).setF32(i).setF33(i).setF34(i).setF35(i).setF36(i).setF37(i).setF38(i).setF39(i).setF40(i)
        .setF41(i).setF42(i).setF43(i).setF44(i).setF45(i).setF46(i).setF47(i).setF48(i).setF49(i).setF50(i)
        .setF51(i).setF52(i).setF53(i).setF54(i).setF55(i).setF56(i).setF57(i).setF58(i).setF59(i).setF60(i)
        .setF61(i).setF62(i).setF63(i).setF64(i).setF65(i).setF66(i).setF67(i).setF68(i).setF69(i).setF70(i)
        .setF71(i).setF72(i).setF73(i).setF74(i).setF75(i).setF76(i).setF77(i).setF78(i).setF79(i).setF80(i)
        .setF81(i).setF82(i).setF83(i).setF84(i).setF85(i).setF86(i).setF87(i).setF88(i).setF89(i).setF90(i)
        .setF91(i).setF92(i).setF93(i).setF94(i).setF95(i).setF96(i).setF97(i).setF98(i).setF99(i).setF100(i);
  };

  private static final RecordGeneratorFactory<Test30String.Builder> TEST_30_STRING = fixedLenByteArraySize -> {
    final Test30String.Builder builder = Test30String.newBuilder();

    return i -> builder
        .setField1("setField1:" + i)
        .setField2("setField2:" + i)
        .setField3("setField3:" + i)
        .setField4("setField4:" + i)
        .setField5("setField5:" + i)
        .setField6("setField6:" + i)
        .setField7("setField7:" + i)
        .setField8("setField8:" + i)
        .setField9("setField9:" + i)
        .setField10("setField10:" + i)
        .setField11("setField11:" + i)
        .setField12("setField12:" + i)
        .setField13("setField13:" + i)
        .setField14("setField14:" + i)
        .setField15("setField15:" + i)
        .setField16("setField16:" + i)
        .setField17("setField17:" + i)
        .setField18("setField18:" + i)
        .setField19("setField19:" + i)
        .setField20("setField20:" + i)
        .setField21("setField21:" + i)
        .setField22("setField22:" + i)
        .setField23("setField23:" + i)
        .setField24("setField24:" + i)
        .setField25("setField25:" + i)
        .setField26("setField26:" + i)
        .setField27("setField27:" + i)
        .setField28("setField28:" + i)
        .setField29("setField29:" + i)
        .setField30("setField30:" + i);
  };

  private static String generateFixedLenStr(int fixedLenByteArraySize) {
    // generate some data for the fixed len byte array field
    char[] chars = new char[fixedLenByteArraySize];
    Arrays.fill(chars, '*');
    return String.copyValueOf(chars);
  }

  private static final Map<Class<? extends Message>, RecordGeneratorFactory<?>> GENERATORS = new HashMap() {
    {
      put(Test1.class, TEST1);
      put(Test30Int32.class, TEST_30_INT32);
      put(Test30String.class, TEST_30_STRING);
      put(Test100Int32.class, TEST_100_INT32);
    }
  };

  public void generateData(
      Path outFile,
      Configuration configuration,
      ParquetProperties.WriterVersion version,
      int blockSize,
      int pageSize,
      int fixedLenByteArraySize,
      CompressionCodecName codec,
      int nRows)
      throws IOException {

    outFile = outFile.suffix(protoClass.getName());

    if (exists(configuration, outFile)) {
      System.out.println("File already exists " + outFile);
      return;
    }

    System.out.println("Generating data @ " + outFile + " with codegenMode " + codegenMode);

    ProtoWriteSupport.setCodegenMode(configuration, codegenMode);
    ProtoWriteSupport.setSchema(configuration, protoClass);

    ParquetWriter<B> writer = ProtoParquetWriter.<B>builder(outFile)
        .withMessage(protoClass)
        .withConf(configuration)
        .withCompressionCodec(codec)
        .withRowGroupSize((long) blockSize)
        .withPageSize(pageSize)
        .enableDictionaryEncoding()
        .withDictionaryPageSize(DICT_PAGE_SIZE)
        .withValidation(false)
        .withWriterVersion(version)
        .build();

    RecordGenerator<B> recordGenerator = recordGeneratorFactory.newRecordGenerator(fixedLenByteArraySize);

    for (int i = 0; i < nRows; i++) {
      writer.write(recordGenerator.apply(i));
    }
    writer.close();
  }
}
