
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
package org.apache.parquet.encryption;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.ShortBufferException;

import org.apache.parquet.bytes.BytesInput;
import org.slf4j.LoggerFactory;

public class DecryptionCodec extends Codec {

	public DecryptionCodec() {
		super(Cipher.DECRYPT_MODE);
		LOG = LoggerFactory.getLogger(DecryptionCodec.class);
	}

	public BytesInput decrypt(BytesInput encryptedBytes) throws CodecFailureException {
		ByteBuffer outputByteBuff = ByteBuffer.allocate((int) encryptedBytes.size());
		try {
			return decrypt(encryptedBytes.toByteBuffer(), outputByteBuff);
		} catch (IllegalBlockSizeException | BadPaddingException | IOException | ShortBufferException e) {
			throw new CodecFailureException("Decription Failed...", e);
		}
	}

	private BytesInput decrypt(ByteBuffer encryptedBytes, ByteBuffer outputByteBuff)
			throws ShortBufferException, IllegalBlockSizeException, BadPaddingException, IOException {
		int size = cipher.doFinal(encryptedBytes, outputByteBuff);
		return BytesInput.from(outputByteBuff, 0, (int) size);
	}

}
