/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.broker.region.cursors;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DestinationStatistics;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.SystemUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author gtully
 * https://issues.apache.org/activemq/browse/AMQ-2020
 **/
public class StoreQueueCursorNoDuplicateTest extends TestCase {
    static final Logger LOG = LoggerFactory.getLogger(StoreQueueCursorNoDuplicateTest.class);
            ActiveMQQueue destination = new ActiveMQQueue("queue-"
            + StoreQueueCursorNoDuplicateTest.class.getSimpleName());
    BrokerService brokerService;

    final static String mesageIdRoot = "11111:22222:0:";
    final int messageBytesSize = 1024;
    final String text = new String(new byte[messageBytesSize]);
    int messageId = 0;
    int dequeueCount = 0;

    private enum Transacted {
        ALWAYS,
        NEVER,
        MIXED
    }

    protected int count = 60;

    @Override
    public void setUp() throws Exception {
        brokerService = createBroker();
        brokerService.setUseJmx(false);
        brokerService.deleteAllMessages();
        brokerService.start();
    }

    protected BrokerService createBroker() throws Exception {
        return new BrokerService();
    }

    @Override
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    public void testNoDuplicateAfterCacheFullAndReadPast() throws Exception {
        final PersistenceAdapter persistenceAdapter = brokerService
                .getPersistenceAdapter();
        final MessageStore queueMessageStore = persistenceAdapter
                .createQueueMessageStore(destination);
        final ConsumerInfo consumerInfo = new ConsumerInfo();
        final DestinationStatistics destinationStatistics = new DestinationStatistics();
        consumerInfo.setExclusive(true);

        final Queue queue = new Queue(brokerService, destination,
                queueMessageStore, destinationStatistics, null);

        queueMessageStore.start();
        queueMessageStore.registerIndexListener(null);

        QueueStorePrefetch underTest = new QueueStorePrefetch(queue, brokerService.getBroker());
        SystemUsage systemUsage = new SystemUsage();
        // ensure memory limit is reached
        systemUsage.getMemoryUsage().setLimit(messageBytesSize * (count + 2));
        underTest.setSystemUsage(systemUsage);
        underTest.setEnableAudit(false);
        underTest.start();
        assertTrue("cache enabled", underTest.isUseCache() && underTest.isCacheEnabled());

        final ConnectionContext contextNotInTx = new ConnectionContext();
        for (int i = 0; i < count; i++) {
            ActiveMQTextMessage msg = getMessage(i);
            msg.setMemoryUsage(systemUsage.getMemoryUsage());

            queueMessageStore.addMessage(contextNotInTx, msg);
            underTest.addMessageLast(msg);
        }

        assertTrue("cache is disabled as limit reached", !underTest.isCacheEnabled());
        int dequeueCount = 0;

        underTest.setMaxBatchSize(2);
        underTest.reset();
        while (underTest.hasNext() && dequeueCount < count) {
            MessageReference ref = underTest.next();
            ref.decrementReferenceCount();
            underTest.remove();
            LOG.info("Received message: {} with body: {}",
                     ref.getMessageId(), ((ActiveMQTextMessage)ref.getMessage()).getText());
            assertEquals(dequeueCount++, ref.getMessageId().getProducerSequenceId());
        }
        underTest.release();
        assertEquals(count, dequeueCount);
    }

    public void testFastSlowConsumersNoDuplicate() throws Exception {
        final int numberOfFlips = 40;
        final PersistenceAdapter persistenceAdapter = brokerService
                .getPersistenceAdapter();
        final MessageStore queueMessageStore = persistenceAdapter
                .createQueueMessageStore(destination);
        final ConnectionContext context = createConnectionContext();
        final Queue queue = new Queue(brokerService, destination,
                queueMessageStore, new DestinationStatistics(), null);

        queueMessageStore.start();
        queueMessageStore.registerIndexListener(null);

        EnumSet<Transacted> transactions = EnumSet.allOf(Transacted.class);

        for (Transacted transacted : transactions) {
            produce(queue, queueMessageStore, context, numberOfFlips, transacted);
            receive(queue, queueMessageStore, context, count * numberOfFlips, numberOfFlips, transacted != Transacted.MIXED);
        }

        assertEquals(count * numberOfFlips * 3, dequeueCount);
    }

    private QueueStorePrefetch createQueuePrefetch(final Queue queue, final int numberOfFlips) throws Exception {
        QueueStorePrefetch underTest = new QueueStorePrefetch(queue, brokerService.getBroker());
        SystemUsage systemUsage = new SystemUsage();

        // ensure memory limit is reached
        systemUsage.getMemoryUsage().setLimit(messageBytesSize * (numberOfFlips / 2 * count));
        underTest.setSystemUsage(systemUsage);
        underTest.setEnableAudit(false);
        underTest.start();

        return underTest;
    }

    private void receive(Queue queue, final MessageStore queueMessageStore, final ConnectionContext context, final int total, final int numberOfFlips, final boolean checkOrder) throws Exception {
        QueueStorePrefetch underTest = createQueuePrefetch(queue, numberOfFlips);
        int dequeuedNow = 0;
        while (underTest.hasNext() && dequeuedNow < total) {
            MessageReference ref = underTest.next();
            ref.decrementReferenceCount();
            underTest.remove();
           // LOG.info("Received message: {} with body: {}",
             //       ref.getMessageId(), ((ActiveMQTextMessage)ref.getMessage()).getText());
            if (checkOrder) {
                assertEquals(dequeueCount++, ref.getMessageId().getProducerSequenceId());
            } else {
                dequeueCount++;
            }
            dequeuedNow++;
            queueMessageStore.removeMessage(context, new MessageAck(ref.getMessage(), MessageAck.STANDARD_ACK_TYPE, 1));
        }
    }

    private ConnectionContext createConnectionContext() {
        final ConnectionContext context = new ConnectionContext();
        context.setConnectionId(new ConnectionId("connection:1"));
        context.setTransactions(new ConcurrentHashMap<>());
        return context;
    }

    private void produce(final Queue queue, final MessageStore queueMessageStore, final ConnectionContext context, final int numberOfFlips, final Transacted transacted) throws Exception {
        QueueStorePrefetch queueStorePrefetch = createQueuePrefetch(queue, numberOfFlips);
        for (int j = 0; j < numberOfFlips; j++) {
            boolean fastConsumers = j % 2 == 0;
            for (int i = 0; i < count; i++) {
                boolean shouldCreateTransaction = transacted == Transacted.ALWAYS || (transacted == Transacted.MIXED && i % 3 == 0);

                ActiveMQTextMessage msg = getMessage(messageId++);
                LocalTransactionId localTransactionId = null;

                if (shouldCreateTransaction) {
                    localTransactionId = new LocalTransactionId(context.getConnectionId(), messageId);
                    brokerService.getBroker().beginTransaction(context, localTransactionId);
                    msg.setTransactionId(localTransactionId);
                }

                msg.setMemoryUsage(queueStorePrefetch.getSystemUsage().getMemoryUsage());

                if (fastConsumers && queueStorePrefetch.isCacheEnabled()) {
                    // Simulate concurrentStoreAndDispatchQueue + fast consumers
                    queueMessageStore.asyncAddQueueMessage(context, msg);
                } else {
                    queueMessageStore.addMessage(context, msg);
                }

                if (localTransactionId != null) {
                    brokerService.getBroker().commitTransaction(context, localTransactionId, true);
                }
                queueStorePrefetch.addMessageLast(msg);
            }

            //Simulate the flip BTW fast and slow consumers
            if (fastConsumers) {
                queueStorePrefetch.waitAsyncMessages();
            }
        }
        queueStorePrefetch.stop();
        queueStorePrefetch.release();
    }

    private ActiveMQTextMessage getMessage(int i) throws Exception {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        MessageId id = new MessageId(mesageIdRoot + i);
        id.setBrokerSequenceId(i);
        id.setProducerSequenceId(i);
        message.setMessageId(id);
        message.setDestination(destination);
        message.setPersistent(true);
        message.setResponseRequired(true);
        message.setText("Msg:" + i + " " + text);
        assertEquals(message.getMessageId().getProducerSequenceId(), i);
        return message;
    }
}
