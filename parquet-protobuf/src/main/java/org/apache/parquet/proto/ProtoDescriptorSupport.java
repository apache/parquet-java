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
import com.google.protobuf.Message;
import com.twitter.elephantbird.util.Protobufs;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.BadConfigurationException;

public class ProtoDescriptorSupport {

  private Descriptors.Descriptor messageDescriptor;
  private Class<? extends Message> protoMessage;

  public ProtoDescriptorSupport(Class<? extends Message> protoMessage) {
    this.protoMessage = protoMessage;
  }

  public ProtoDescriptorSupport(Descriptors.Descriptor messageDescriptor) {
    this.messageDescriptor = messageDescriptor;
  }

  public Descriptors.Descriptor getMessageDescriptor(Configuration configuration) {
    // if no protobuf descriptor was given in constructor, load descriptor from configuration (set with setProtobufClass)
    if (protoMessage == null && messageDescriptor == null) {
      Class<? extends Message> pbClass = configuration.getClass(ProtoWriteSupport.PB_CLASS_WRITE, null, Message.class);
      if (pbClass != null) {
        protoMessage = pbClass;
      } else {
        String msg = "Protocol buffer class not specified.";
        String hint = " Please use method ProtoParquetOutputFormat.setProtobufClass(...) or other similar method.";
        throw new BadConfigurationException(msg + hint);
      }
    }

    if (messageDescriptor == null) {
      messageDescriptor = Protobufs.getMessageDescriptor(protoMessage);
    }
    return messageDescriptor;
  }
}
