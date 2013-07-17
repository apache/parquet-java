package parquet.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

/**
 * reads one record from an input and writes it to an ouput
 *
 * @author Julien Le Dem
 *
 */
public interface ProtocolPipe {
    void readOne(TProtocol in, TProtocol out) throws TException;
}
