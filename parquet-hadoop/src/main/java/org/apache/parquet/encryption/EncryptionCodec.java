package org.apache.parquet.encryption;

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
import java.io.IOException;
import java.nio.ByteBuffer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.parquet.bytes.BytesInput;

public class EncryptionCodec extends Codec {

	public EncryptionCodec() {
		super(Cipher.ENCRYPT_MODE);
	}

	public BytesInput encrypt(BytesInput compressedBytes) throws CodecFailureException {
		BytesInput bytesInput;
		try {
			int outputSize = cipher.getOutputSize((int) compressedBytes.size());
			ByteBuffer outputByteBuff = ByteBuffer.allocate(outputSize);
			int size = cipher.doFinal(compressedBytes.toByteBuffer(), outputByteBuff);
			bytesInput = BytesInput.from(outputByteBuff, 0, (int) size);
			System.out.println("**********Encrypted size:"+bytesInput.size());
		} catch (IllegalBlockSizeException | BadPaddingException | ShortBufferException | IOException e) {
			throw new CodecFailureException("Encription Failed...", e);
		}
		return bytesInput;
	}
}
