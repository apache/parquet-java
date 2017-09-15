package org.apache.parquet.encryption;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.security.Key;
import java.security.KeyStore;
import java.util.Enumeration;
import java.util.Properties;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Codec {

	private static final String ALGO = "AES";
	private static final byte[] keyValue = new byte[] { 'T', 'h', 'e', 'B', 'e', 's', 't', 'S', 'e', 'c', 'r', 'e', 't',
			'K', 'e', 'y' };
	private final int codecMode;
	protected Cipher cipher;
	protected Logger LOG = LoggerFactory.getLogger(Codec.class);

	public Codec(int codecMode) {
		super();
		this.codecMode = codecMode;
		try {
			initialize();
		} catch (Exception e) {
			throw new RuntimeException("Codec Initialization failed", e);
		}
	}

	private void initialize() throws Exception {
		Key key;
		synchronized (Codec.class) {
			key = generateKey();
		}
		cipher = Cipher.getInstance(ALGO);
		cipher.init(codecMode, key);
	}

	private Key getKey() throws IOException {
		String keyStoreProps = "keystore.properties";
		InputStream is = this.getClass().getClassLoader().getResourceAsStream(keyStoreProps);
		Properties props = new Properties();
		props.load(is);
		if (is != null) {
			is.close();
		}
		String keyPass = props.getProperty("key-password");
		String jksFilePath = props.getProperty("jks-filepath");
		if ((keyPass == null || keyPass.isEmpty()) || (jksFilePath == null || jksFilePath.isEmpty())) {
			LOG.warn("Please check '" + keyStoreProps
					+ "'; Required values for 'key-password' & 'jks-filepath'. Values follows,");
			Enumeration<Object> em = props.keys();
			while (em.hasMoreElements()) {
				String key = (String) em.nextElement();
				String value;
				if (key.equals("key-password")) {
					value = props.get(key).toString().isEmpty() ? "" : "******";
				} else {
					value = props.get(key).toString();
				}
				LOG.warn("{}={}", key, value);
			}
			return null;
		}
		Key key = null;
		try {
			key = getKeyFromJKS(keyPass, jksFilePath);
		} catch (Exception e) {
			LOG.warn("Key retrieval from JKS failed", e);
		}
		return key;
	}

	private Key getKeyFromJKS(String password, String jksFilePath) throws Exception {
		KeyStore keystore = KeyStore.getInstance("jceks");
		String alias = "parquet";
		InputStream is = new FileInputStream(new File(jksFilePath));
		char[] passCharArray = password.toCharArray();
		keystore.load(is, passCharArray);
		if (is != null) {
			is.close();
		}
		Key key = keystore.getKey(alias, passCharArray);
		return key;
	}

	private Key generateKey() throws Exception {
		Key key = null;
		try {
			key = getKey();
		} catch (Exception e) {
			LOG.warn("Key password retrieval failed for file 'keystore.properties'");
		}
		if (key != null) {
			return key;
		} else {
			LOG.warn("Fallback to default key");
			key = new SecretKeySpec(keyValue, ALGO);
			return key;
		}

	}
}
