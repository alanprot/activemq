package org.apache.activemq.broker;

import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.util.Wait;

import java.util.ArrayList;

public class BrokerCursorOutOfSyncTestSupport extends BrokerRestartTestSupport {
    public boolean restart;

    public static Test suite() {
        return suite(BrokerCursorOutOfSyncTestSupport.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestMessageOrderBtwTransactedAndNonTransactedSends() {
        this.addCombinationValues("restart", new Boolean[] {true, false});
    }

    public void testMessageOrderBtwTransactedAndNonTransactedSends() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        ArrayList<String> expected = new ArrayList<String>();
        int MESSAGE_COUNT = 10000;

        final StubConnection producerConnection = createConnection();
        final ConnectionInfo producerConnectionInfo = createConnectionInfo();
        producerConnection.send(producerConnectionInfo);

        new Thread(() -> produceMessages(producerConnection, producerConnectionInfo, destination, true, MESSAGE_COUNT / 2, expected)).start();
        new Thread(() -> produceMessages(producerConnection, producerConnectionInfo, destination, false, MESSAGE_COUNT / 2, expected)).start();

        assertTrue("Produce all messages", Wait.waitFor(() -> expected.size() == MESSAGE_COUNT));
        producerConnection.request(closeConnectionInfo(producerConnectionInfo));

        // restart the broker.
        if (restart) {
            restartBroker();
        }

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(consumerInfo);

        for(int i=0; i < MESSAGE_COUNT; i++) {
            Message m = receiveMessage(connection);
            assertNotNull("Should have received message "+expected.get(i)+" by now!", m);
            assertEquals(expected.get(i), m.getMessageId().toString());
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            connection.send(ack);
        }

        connection.request(closeConnectionInfo(connectionInfo));
    }

    private void produceMessages(final StubConnection connection, final ConnectionInfo connectionInfo, final ActiveMQDestination destination, final Boolean transacted, final int messageCount, final ArrayList<String> expected) {
        int producedMessages = 0;
        while (producedMessages != messageCount) {
            try {
                // Setup the producer and send the message.
                SessionInfo sessionInfo = createSessionInfo(connectionInfo);
                ProducerInfo producerInfo = createProducerInfo(sessionInfo);
                connection.send(sessionInfo);
                connection.send(producerInfo);
                while (producedMessages != messageCount) {
                    synchronized (this) {
                        LocalTransactionId txid = null;
                        if (transacted) {
                            txid = createLocalTransaction(sessionInfo);
                            connection.send(createBeginTransaction(connectionInfo, txid));
                        }

                        Message message = createMessage(producerInfo, destination);
                        message.setTransactionId(txid);
                        message.setPersistent(true);

                        connection.send(message);

                        if (txid != null) {
                            connection.send(createCommitTransaction1Phase(connectionInfo, txid));
                        }

                        expected.add(message.getMessageId().toString());
                        producedMessages++;
                    }

                }
            } catch (Exception ex) {
                //Ignored
            }
        }
    }
}
