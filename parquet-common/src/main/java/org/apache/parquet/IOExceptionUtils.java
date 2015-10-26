package org.apache.parquet;

import java.io.Closeable;
import java.io.IOException;

/**
 * Utilities for managing I/O resources.
 */
public class IOExceptionUtils {

	/**
	 * Call the #close() method on a {@see Closable}, wrapping any IOException
	 * in a runtime exception.
	 *
	 * @param closeable - resource to close
	 */
	public static void closeQuietly(Closeable closeable) {
		try {
			closeable.close();
		} catch(IOException e) {
			throw new ParquetRuntimeException("Error closing I/O related resources.", e) {};
		}
	}

}
