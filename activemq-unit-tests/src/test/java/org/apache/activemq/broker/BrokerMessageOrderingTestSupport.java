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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class BrokerMessageOrderingTestSupport extends BrokerRestartTestSupport {
    public boolean transacted;
    public boolean restart;

    public static Test suite() {
        return suite(BrokerMessageOrderingTestSupport.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestLargeQueuePersistentMessagesMantainOrderOnRestart() {
        this.addCombinationValues("transacted", new Boolean[] {true, false});
        this.addCombinationValues("restart", new Boolean[] {true, false});
    }

    public void testLargeQueuePersistentMessagesMantainOrderOnRestart() throws Exception {
        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        ArrayList<String> expected = new ArrayList<String>();
        int MESSAGE_COUNT = 10000;

        final StubConnection producerConnection = createConnection();
        final ConnectionInfo producerConnectionInfo = createConnectionInfo();
        producerConnection.send(producerConnectionInfo);

        // Simulate slow and fast consumers
        Thread consumerThread = new Thread(() -> registerConsumer(destination));
        consumerThread.start();

        produceMessages(producerConnection, producerConnectionInfo, destination, transacted,MESSAGE_COUNT, expected);

        assertTrue("Produce all messages", Wait.waitFor(() -> expected.size() == MESSAGE_COUNT));
        producerConnection.request(closeConnectionInfo(producerConnectionInfo));
        consumerThread.interrupt();

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

    private void registerConsumer(final ActiveMQDestination destination) {
        try {
            while (true) {
                StubConnection connection = createConnection();
                ConnectionInfo connectionInfo = createConnectionInfo();
                SessionInfo sessionInfo = createSessionInfo(connectionInfo);
                ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
                consumerInfo.setPrefetchSize(100000);
                connection.send(connectionInfo);
                connection.send(sessionInfo);
                connection.send(consumerInfo);
                receiveMessage(connection);
                connection.request(closeConnectionInfo(connectionInfo));
                Thread.sleep(50);
            }
        } catch (Exception ex) {
            //Ignored
        }
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
