package parquet.column.values.deltastrings;

import java.util.Arrays;

import parquet.Log;
import parquet.bytes.BytesInput;
import parquet.column.Encoding;
import parquet.column.values.ValuesWriter;
import parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import parquet.column.values.plain.PlainValuesWriter;
import parquet.io.api.Binary;

public class DeltaStringValuesWriter extends ValuesWriter{
	
	private static final Log LOG = Log.getLog(DeltaStringValuesWriter.class);
	
	private ValuesWriter prefixLengthWriter;
	private ValuesWriter suffixWriter;
	private byte[] previous;
	
	public DeltaStringValuesWriter(int initialCapacity) {
		this.prefixLengthWriter = new PlainValuesWriter(initialCapacity);
		this.suffixWriter = new DeltaLengthByteArrayValuesWriter(initialCapacity);
		this.previous = null;
	}

	@Override
	public long getBufferedSize() {
		return prefixLengthWriter.getBufferedSize() + suffixWriter.getBufferedSize();
	}

	@Override
	public BytesInput getBytes() {
		return BytesInput.concat(prefixLengthWriter.getBytes(), suffixWriter.getBytes());
	}

	@Override
	public Encoding getEncoding() {
		return Encoding.DELTA_STRINGS;
	}

	@Override
	public void reset() {
		prefixLengthWriter.reset();
		suffixWriter.reset();
	}

	@Override
	public long getAllocatedSize() {
		return prefixLengthWriter.getAllocatedSize() + suffixWriter.getAllocatedSize();
	}

	@Override
	public String memUsageString(String prefix) {
		prefix = prefixLengthWriter.memUsageString(prefix);
		return suffixWriter.memUsageString(prefix + "  DELTA_STRINGS");
	}
	
	@Override
	public void writeBytes(Binary v) {
		int i = 0;
		byte[] vb = v.getBytes();
		if (previous != null) {
		  int length = previous.length < vb.length ? previous.length : vb.length;
		  for(i = 0; (i < length) && (previous[i] == vb[i]); i++);
		}
		prefixLengthWriter.writeInteger(i);
		// TODO figure this out
		suffixWriter.writeBytes(Binary.fromByteArray(Arrays.copyOfRange(vb, i, vb.length)));
		previous = vb;
	}
}
