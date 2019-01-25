package org.apache.activemq.store.kahadb.disk.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BooleanMarshaller implements Marshaller<Boolean> {

    public static final BooleanMarshaller INSTANCE = new BooleanMarshaller();

    @Override
    public void writePayload(final Boolean object, final DataOutput dataOut) throws IOException {
        dataOut.writeBoolean(object);
    }

    @Override
    public Boolean readPayload(final DataInput dataIn) throws IOException {
        return dataIn.readBoolean();
    }

    @Override
    public int getFixedSize() {
        return 1;
    }

    @Override
    public boolean isDeepCopySupported() {
        return true;
    }

    @Override
    public Boolean deepCopy(final Boolean source) {
        return source;
    }
}
