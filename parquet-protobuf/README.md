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

Protocol Buffer support for Parquet columnar format.

## Message fields stored as proto bytes

Two kinds of message fields cannot be mapped to a Parquet group, so `ProtoSchemaConverter`
terminates them as the **serialized protobuf message** instead:

* **Fields of an empty message type** (`message Stub {}`) &mdash; Parquet forbids empty groups.
  An empty message serializes to zero bytes, so the column is cheap, and field presence still
  round-trips: `null` means the field was unset, an empty value means it was set.
* **Recursive fields beyond `parquet.proto.maxRecursion`** (default 5) &mdash; the remaining
  sub-tree is stored as the serialized message instead of expanding the schema forever.

The Parquet type is an unannotated `BINARY` column that keeps the field's own repetition (or,
for repeated fields and map values, sits inside the standard `LIST` / `MAP` wrappers when
`parquet.proto.writeSpecsCompliant` is set):

```
message Trees.StubBox {
  optional binary stub = 1;                    // Stub stub = 1;
  optional group stubs (LIST) = 2 {            // repeated Stub stubs = 2;
    repeated group list {
      required binary element;
    }
  }
  optional group stub_map (MAP) = 3 {          // map<string, Stub> stub_map = 3;
    repeated group key_value {
      required binary key (STRING);
      optional binary value;
    }
  }
}
```

Readers that do not know about protobuf simply see opaque bytes (all of them empty for an
empty message type). Readers that have the generated message class can parse the bytes back into
the message; `ProtoParquetReader` does this automatically, resolving the class from the
`parquet.proto.class` footer key (or from the class configured for reading). The writer also stores
the message descriptor in the footer under `parquet.proto.descriptor`, which tools that do not
have the generated class can use to interpret the bytes; `ProtoParquetReader` itself does not read
it.

Note that the column type follows the proto schema at write time: if an empty message type later
gains fields, or `parquet.proto.maxRecursion` is changed, new files store the field as a group
where old files store `BINARY`. Tools that merge schemas across such files will report a type
conflict, the same way they do for any other field whose type changed.
