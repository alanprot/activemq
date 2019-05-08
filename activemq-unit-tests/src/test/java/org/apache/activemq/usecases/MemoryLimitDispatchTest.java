package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ProducerThread;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

public class MemoryLimitDispatchTest extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryLimitDispatchTest.class);
    final byte[] payload = new byte[100 * 1024]; //100KB
    private BrokerService broker;

    protected static BrokerService createBroker() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.getSystemUsage().getMemoryUsage().setLimit(1024 * 1024); //1MB
        brokerService.deleteAllMessages();
        brokerService.setPersistent(true);

        setPersistenceAdapter(brokerService, TestSupport.PersistenceAdapterChoice.KahaDB);
        brokerService.getPersistenceAdapter().deleteAllMessages();

        brokerService.start();

        return brokerService;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        broker.start();
        broker.waitUntilStarted();
    }

    @Test(timeout = 640000)
    public void testDispatchFullCusor() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");


        Connection conn = factory.createConnection();
        conn.start();
        Session sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Queue queueStore = sess.createQueue("STORE");
        Queue queueToFill = sess.createQueue("FILL");
        produce(sess, queueStore);
        produce(sess, queueToFill);
        Thread.sleep(1000);
        conn.stop();

        restart();

        conn = factory.createConnection();
        conn.start();
        sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        consumer(sess, queueToFill);
        consumer(sess, queueStore);
    }

    private void restart() throws Exception {
        broker.stop();
        broker.waitUntilStopped();

        broker = createBroker();

        broker.start();
        broker.waitUntilStarted();
        broker.getSystemUsage().getMemoryUsage().setLimit(1024 * 1024); //1MB
    }

    private void consumer(final Session sess, final Queue queue) throws JMSException {
        // consume one message
        MessageConsumer consumer = sess.createConsumer(queue);
        Message msg = consumer.receive(50000);
        msg.acknowledge();
    }

    private void produce(final Session sess, final Queue queue) throws InterruptedException {
        final ProducerThread producer = new ProducerThread(sess, queue) {
            @Override
            protected Message createMessage(int i) throws Exception {
                BytesMessage bytesMessage = session.createBytesMessage();
                bytesMessage.writeBytes(payload);
                return bytesMessage;
            }
        };
        producer.setPersistent(true);
        producer.setMessageCount(200);
        producer.start();
        producer.join();
    }
}
