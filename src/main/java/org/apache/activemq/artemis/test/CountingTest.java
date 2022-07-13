package org.apache.activemq.artemis.test;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.*;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;

public class CountingTest {
    private static final int COUNT_N = 15;
    private static final String TARGET_QUEUE = "__syn_target_queue";
    private static final String DESTINATION_QUEUE = "my_destination_queue";
    private static final String TRIGGER_QUEUE = "__trg";

    public static void main(String[] args) throws Exception {
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://127.0.0.1:61616");

        ClientSessionFactory factory = locator.createSessionFactory();
        ClientSession session = factory.createSession();

        QueueConfiguration queueConfiguration;

        // Create target queue
        queueConfiguration = new QueueConfiguration();
        queueConfiguration.setName(TARGET_QUEUE);
//        queueConfiguration.setAddress("queues/example");
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        queueConfiguration.setDurable(false);
        session.createQueue(queueConfiguration);

        // Create destination queue
        queueConfiguration = new QueueConfiguration();
        queueConfiguration.setName(DESTINATION_QUEUE);
//        queueConfiguration.setAddress("queues/example");
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        queueConfiguration.setDurable(false);
        session.createQueue(queueConfiguration);

        // Create trigger
        ClientProducer triggerProducer = session.createProducer(TRIGGER_QUEUE);
        ClientMessage triggerMessage = session.createMessage(false);

        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packString(TARGET_QUEUE).packString(DESTINATION_QUEUE).packInt(COUNT_N);
        packer.close();

        StringBuilder sb = new StringBuilder(packer.toByteArray().length * 2);
        for (byte b : packer.toByteArray())
            sb.append(String.format("%02x", b));
        System.out.println(sb);

        triggerMessage.getBodyBuffer().writeBytes(packer.toByteArray());

//        triggerMessage.getBodyBuffer().writeString("target=" + TARGET_QUEUE + ";destination=" + DESTINATION_QUEUE + ";count=" + COUNT_N);

        triggerProducer.send(triggerMessage);

        // Create destination consumer
        ClientConsumer consumer = session.createConsumer("my_destination_queue");

        session.start();
        Thread consumerThread = new Thread(() -> {
            try {
                System.out.println("Starting to consume...");
                ClientMessage msg = consumer.receive();
                String result = msg.getBodyBuffer().readString();
                System.out.println(result);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        });
        consumerThread.start();

        Thread.sleep(2500);

        // Create messages
        ClientProducer producer = session.createProducer(TARGET_QUEUE);
        ClientMessage message = session.createMessage(false);
        message.getBodyBuffer().writeString("Hello");

        System.out.println("Sending messages now");
        long t0 = System.currentTimeMillis();
        for (int i = 0; i < COUNT_N; i++) {
            producer.send(message);
        }
        long t1 = System.currentTimeMillis();

        long send_elapsed = t1 - t0;
        System.out.println("Sending " + COUNT_N + " messages took " + send_elapsed + " ms (" + send_elapsed / 1000.0 +
                " s)");
        System.out.println("Throughput " + ((float) COUNT_N) / (send_elapsed / 1000.0) + " msg/s");

        consumerThread.join();
        consumer.close();
        session.deleteQueue(TARGET_QUEUE);
        session.deleteQueue(DESTINATION_QUEUE);
        session.close();
    }
}
