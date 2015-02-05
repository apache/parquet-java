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
package parquet.proto;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class TestUtils {

  public static Path someTemporaryFilePath() throws IOException {
    File tmp = File.createTempFile("ParquetProtobuf_unitTest", ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    return new Path(tmp.getPath());
  }

  public static <T extends MessageOrBuilder> List<T> writeAndRead(T... records) throws IOException {
    Class<? extends Message> cls = inferRecordsClass(records);

    Path file = writeMessages(cls, records);

    return readMessages(file);
  }

  public static Class<? extends Message> inferRecordsClass(MessageOrBuilder[] records) {
    Class<? extends Message> cls = null;

    for (MessageOrBuilder record : records) {
      Class<? extends Message> recordClass;
      if (record instanceof Message.Builder) {
        recordClass = ((Message.Builder) record).build().getClass();
      } else if (record instanceof Message) {
        recordClass = ((Message) record).getClass();
      } else {
        throw new RuntimeException("Illegal class " + record);
      }

      if (cls == null) {
        cls = recordClass;
      } else if (!cls.equals(recordClass)) {
        throw new RuntimeException("Class mismatch :" + cls + " and " + recordClass);
      }
    }
    return cls;
  }

  /**
   * Writes messages to file, reads messages from file and checks if everything is OK.
   */
  public static <T extends  MessageOrBuilder> List<T> testData(T... messages) throws IOException {

    checkSameBuilderInstance(messages);

    List<MessageOrBuilder> input = cloneList(messages);

    List<MessageOrBuilder> output = (List<MessageOrBuilder>) writeAndRead(messages);

    List<Message> outputAsMessages = asMessages(output);
    assertEquals("The protocol buffers are not same:\n", asMessages(input), outputAsMessages);
    return (List<T>) outputAsMessages;
  }

  private static List<MessageOrBuilder> cloneList(MessageOrBuilder[] messages) {
    List<MessageOrBuilder> result = new ArrayList<MessageOrBuilder>();

    for (MessageOrBuilder mob : messages) {
      result.add(asMessage(mob));
    }

    return result;
  }

  public static List<Message> asMessages(List<MessageOrBuilder> mobs) {
    List<Message> result = new ArrayList<Message>();
    for (MessageOrBuilder messageOrBuilder : mobs) {
      result.add(asMessage(messageOrBuilder));
    }

    return result;
  }

  /**
   * Given message or builder returns same data as message
   */
  public static Message asMessage(MessageOrBuilder mob) {
    Message message;
    if (mob instanceof Message.Builder) {
      message = ((Message.Builder) mob).build();
    } else {
      message = (Message) mob;
    }
    return message;
  }

  /**
   * Fails if some instance of builder is two times in list.
   */
  private static void checkSameBuilderInstance(MessageOrBuilder[] messages) {
    for (int i = 0; i < messages.length; i++) {
      MessageOrBuilder firstMessage = messages[i];
      boolean isBuilder = firstMessage instanceof Message.Builder;

      if (isBuilder) {
        for (int j = 0; j < messages.length; j++) {
          MessageOrBuilder secondMessage = messages[j];

          if (i != j) {
            boolean isSame = secondMessage == firstMessage;
            if (isSame) {
              fail("Data contains two references to same instance." + secondMessage);
            }
          }
        }
      }
    }
  }

  /**
   * Reads messages from given file. The file could/should be created by method writeMessages
   */
  public static <T extends MessageOrBuilder> List<T> readMessages(Path file) throws IOException {
    ProtoParquetReader<T> reader = new ProtoParquetReader<T>(file);

    List<T> result = new ArrayList<T>();
    boolean hasNext = true;
    while (hasNext) {
      T item = reader.read();
      if (item == null) {
        hasNext = false;
      } else {
        assertNotNull(item);
        // It makes sense to return message but production code wont work with messages
        result.add((T) asMessage(item).toBuilder());
      }
    }
    reader.close();
    return result;
  }

  /**
   * Writes messages to temporary file and returns its path.
   */
  public static Path writeMessages(MessageOrBuilder... records) throws IOException {
    return writeMessages(inferRecordsClass(records), records);
  }

  public static Path writeMessages(Class<? extends Message> cls, MessageOrBuilder... records) throws IOException {
    Path file = someTemporaryFilePath();

    ProtoParquetWriter<MessageOrBuilder> writer =
            new ProtoParquetWriter<MessageOrBuilder>(file, cls);

    for (MessageOrBuilder record : records) {
      writer.write(record);
    }

    writer.close();

    return file;
  }

}
