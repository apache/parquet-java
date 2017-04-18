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

import org.apache.parquet.bytes.BytesInput;
import org.junit.Assert;
import org.junit.Test;

public class TestEncryption {

	BytesEncryptor encryptor = new BytesEncryptor();
	BytesDecryptor decryptor = new BytesDecryptor();

	@Test
	public void codecTest() throws IOException, CodecFailureException {
		String strValue = "Hai Java World";
		BytesInput encrypted = encrypt(strValue);
		String decrypted = decrypt(encrypted);
		Assert.assertEquals(strValue, decrypted);
	}

	private String decrypt(BytesInput compressedBytes) throws CodecFailureException, IOException {
		BytesInput actualdata = decryptor.decrypt(compressedBytes);
		byte[] byteArray = actualdata.toByteArray();
		String result = new String(byteArray, "UTF8");
		return result;
	}

	private BytesInput encrypt(String value) throws IOException, CodecFailureException {
		byte[] bytes = value.getBytes();
		ByteBuffer buff = ByteBuffer.allocate(bytes.length);
		buff.put(bytes);
		BytesInput bytesInput = toBytesInput(buff, bytes.length);
		BytesInput encryptedOutput = encryptor.encrypt(bytesInput);
		return encryptedOutput;
	}

	public BytesInput toBytesInput(ByteBuffer byteBuf, int size) throws IOException {
		// int pos = byteBuf.position();
		int pos = 0;
		final BytesInput r = BytesInput.from(byteBuf, pos, size);
		byteBuf.position(pos + size);
		return r;
	}

}
