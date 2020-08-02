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
package org.apache.parquet.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Descriptors.Descriptor;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.proto.test.TestProtobuf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class ProtoDescriptorSupportTest {

  @Test
  public void testProtoMessageClassConstructor() throws Exception {
    Configuration conf =  new Configuration();
    ProtoDescriptorSupport prs = new ProtoDescriptorSupport(TestProtobuf.InnerMessage.class);
    Descriptors.Descriptor msgDesc = prs.getMessageDescriptor(conf);
    Descriptors.Descriptor msgDesc2 = Protobufs.getMessageDescriptor(TestProtobuf.InnerMessage.class);
    assertEquals(msgDesc,msgDesc2 );
  }

  @Test
  public void testProtoDescriptorConstructor() throws Exception {
    Configuration conf =  new Configuration();
    ProtoDescriptorSupport prs = new ProtoDescriptorSupport(TestProtobuf.InnerMessage.getDescriptor());
    Descriptors.Descriptor msgDesc = prs.getMessageDescriptor(conf);
    Descriptors.Descriptor msgDesc2 = Protobufs.getMessageDescriptor(TestProtobuf.InnerMessage.class);
    assertEquals(msgDesc,msgDesc2 );
  }

  @Test
  public void testProtoMessageClassWithConfiguration() throws Exception {
    Configuration conf =  new Configuration();
    conf.setClass(ProtoWriteSupport.PB_CLASS_WRITE, TestProtobuf.InnerMessage.class, Message.class);
    Descriptor protoDescriptor = null;
    ProtoDescriptorSupport prs = new ProtoDescriptorSupport(protoDescriptor);
    Descriptors.Descriptor msgDesc = prs.getMessageDescriptor(conf);
    Descriptors.Descriptor msgDesc2 = Protobufs.getMessageDescriptor(TestProtobuf.InnerMessage.class);
    assertEquals(msgDesc,msgDesc2 );
  }

  @Test(expected = BadConfigurationException.class)
  public void testShouldThrowBadConfigurationExceptionIfPBWriteClassNotSet() throws Exception {
    Configuration conf =  new Configuration();
    Descriptor protoDescriptor = null;
    ProtoDescriptorSupport prs = new ProtoDescriptorSupport(protoDescriptor);
    prs.getMessageDescriptor(conf);
  }

}
