#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Generates the arrow-v2-*.parquet fixtures used by TestDataPageV2EncryptionInterop.

The files are written by Arrow C++ (through pyarrow) with Parquet Modular Encryption and DataPageV2
pages. Arrow encrypts the whole page body of a DataPageV2 page, repetition and definition levels
included, which is the layout the parquet-java reader has to understand.

The rows must match DataPageV2EncryptionTestData.row(i) in the Java test sources. The keys are
wrapped by a toy KMS client (base64 only) that is mirrored by Base64KmsClient in the Java tests.

Usage: python3 generate_arrow_fixtures.py <output-dir>     (generated with pyarrow 24.0.0)
"""

import base64
import sys
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.parquet.encryption as pe

NUM_ROWS = 500
FOOTER_KEY_ID = "footkey"


class Base64KmsClient(pe.KmsClient):
    """Not a secure KMS: wrapping a key is just base64 encoding it. For test fixtures only."""

    def __init__(self, kms_connection_configuration):
        pe.KmsClient.__init__(self)

    def wrap_key(self, key_bytes, master_key_identifier):
        return base64.b64encode(key_bytes).decode("ascii")

    def unwrap_key(self, wrapped_key, master_key_identifier):
        return base64.b64decode(wrapped_key)


def table():
    seq, ids, names, tags = [], [], [], []
    for i in range(NUM_ROWS):
        seq.append(i)
        ids.append(None if i % 7 == 0 else i)
        names.append(None if i % 5 == 0 else "name-%d" % i)
        if i % 4 == 0:
            tags.append(None)
        else:
            tags.append([None if (i + j) % 3 == 0 else "tag-%d-%d" % (i, j) for j in range(i % 4)])
    schema = pa.schema([
        pa.field("seq", pa.int64(), nullable=False),
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("tags", pa.list_(pa.field("element", pa.string()))),
    ])
    return pa.table(
        {"seq": seq, "id": pa.array(ids, pa.int32()), "name": names, "tags": tags}, schema=schema)


def main(out_dir):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    factory = pe.CryptoFactory(lambda config: Base64KmsClient(config))
    kms_config = pe.KmsConnectionConfig()
    for algorithm, algorithm_name in (("AES_GCM_V1", "aes-gcm-v1"), ("AES_GCM_CTR_V1", "aes-gcm-ctr-v1")):
        for codec in ("none", "snappy"):
            encryption = pe.EncryptionConfiguration(
                footer_key=FOOTER_KEY_ID,
                uniform_encryption=True,
                encryption_algorithm=algorithm,
                double_wrapping=False,
                internal_key_material=True,
                cache_lifetime=timedelta(minutes=1),
            )
            properties = factory.file_encryption_properties(kms_config, encryption)
            path = out / ("arrow-v2-%s-%s.parquet" % (algorithm_name, "uncompressed" if codec == "none" else codec))
            with pq.ParquetWriter(
                    str(path), table().schema, encryption_properties=properties,
                    data_page_version="2.0", compression=codec, write_batch_size=100,
                    data_page_size=1, use_dictionary=False) as writer:
                writer.write_table(table())
            print("wrote", path, path.stat().st_size)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
