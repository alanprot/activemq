package org.apache.activemq.store.kahadb.disk.page;

import junit.framework.TestCase;
import org.apache.activemq.store.kahadb.disk.util.Marshaller;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

public class TransactionTest  extends TestCase {
    public void testHugeTransaction() throws IOException {
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnablePageCaching(false);
        pf.load();
        System.setProperty("maxKahaDBTxSize", "" + (1024*1024));
        int numberOfBytesToWrite = 1024*1024*2;

        Transaction tx = pf.tx();
        Page<byte[]> page = tx.allocate();
        byte[] bytes = new byte[numberOfBytesToWrite];

        Marshaller<byte[]> marshaller = new Marshaller<byte[]>() {
            @Override
            public void writePayload(final byte[] object, final DataOutput dataOut) throws IOException {
                dataOut.write(object);
            }

            @Override
            public byte[] readPayload(final DataInput dataIn) throws IOException {
                byte[] readBytes = new byte[numberOfBytesToWrite];
                for (int i = 0; i < numberOfBytesToWrite; i++) {
                    readBytes[i] = dataIn.readByte();
                }

                return readBytes;
            }

            @Override
            public int getFixedSize() {
                return 0;
            }

            @Override
            public boolean isDeepCopySupported() {
                return false;
            }

            @Override
            public byte[] deepCopy(final byte[] source) {
                return new byte[0];
            }
        };

        for (int i = 0; i < numberOfBytesToWrite; i++) {
            bytes[i] = (byte) (i % 8);
        }

        page.set(bytes);

        tx.store(page, marshaller, true);

        tx.commit();
        pf.flush();

        tx = pf.tx();

        page = tx.load(page.getPageId(), marshaller);

        for (int i = 0; i < numberOfBytesToWrite; i++) {
            assertEquals(bytes[i], page.get()[i]);
        }

    }
}
