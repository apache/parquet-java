<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  -->

parquet-protobuf
================

### What are protocol buffers?
Protocol buffers are Google's language-neutral, platform-neutral, extensible mechanism for serializing structured data – think XML, but smaller, faster, and simpler. You define how you want your data to be structured once, then you can use special generated source code to easily write and read your structured data to and from a variety of data streams and using a variety of languages.

[Google Protocol Buffers](https://developers.google.com/protocol-buffers)

### What is Parquet/Protobuf integration?

This library provides convenient tools to easily serialize Google protocol buffers (protobufs) into Apache Parquet files and deserialize them from Apache Parquest files.

### Usage

#### Details

When the parquet-protobuf library is used to write Parquet files, the Parquet files are enriched with the following metadata:

Meta Key | Meta Value | Description
------------ | ------------- | ------------- |
parquet.proto.class               | org.apache.parquet.proto.Document | The class of the protobuf contained within the file |
parquet.proto.type                | type.googleapis.com/org.apache.parquet.proto.Document | URL for a given message type (default: type.googleapis.com/_packagename_._messagename_) |
parquet.proto.descriptor          | {...} | Textual (JSON) representation of the Protocol Message |
parquet.proto.writeSpecsCompliant | true  | Parquet file was written using spec compliant schemas (default: true) | 

#### Write a Google protocol buffer
```java
    org.apache.hadoop.fs.Path path = ...;
    List<Document> documents = ...;

    // Document is a Google protocol buffer (generated)
    try (ParquetWriter<Document> writer = ProtoParquetWriter
        .builder(path, Document.getDefaultInstance()).build()) {
      for (Document document : documents) {
        writer.write(document);
      }
    }
```

#### Reading Google protocols buffer from Parquet file (generic)
```java
    org.apache.hadoop.fs.Path path = ...;
    List<Document> results = new ArrayList<>();

    /*
     * Parquet reader will use Document protocol buffer to decode
     * despite what is written in the file's meta data. This can be
     * used to read Parquet files that were not written with the
     * parquet-protobuf library
     */
    try (ParquetReader<Document> reader = ProtoParquetReader
        .builder(path, Document.getDefaultInstance()).build()) {
      Iterator<Document> iter = ProtoParquetIterator.wrap(reader);
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }
```

#### Reading Google protocol buffers from Parquet file (parquet-protobuf)

```
    org.apache.hadoop.fs.Path path = ...;
    List<Document> results = new ArrayList<>();

    /*
     * The Google protocol buffer class information is read from the Parquet file
     * meta data and then cast into the appropriate class.
     */
    try (ParquetReader<MessageOrBuilder> reader =
        ProtoParquetReader.builder(path).build()) {
      Iterator<Document> iter = ProtoParquetMessageIterator.wrap(reader);
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    /*
     * The Google protocol buffer class information is read from the Parquet file
     * meta data and then can be acted upon in a generic way
     */
    OutputStream output = ...;
    try (ParquetReader<MessageOrBuilder> reader =
        ProtoParquetReader.builder(path).build()) {
      while(true) {
        MessageOrBuilder message = this.reader.read();
        if (message == null) {
            return;
        } else {
            if (message instanceof Builder) {
                ((Builder) message).build().writeTo(output);
            } else if (message instanceof Message) {
                ((Message)message).writeTo(output);
            }
        }
      }
    }
```