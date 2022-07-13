/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.test;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.client.*;

public class ProducerExample {

    static final int COUNT_THRESHOLD = 1;

    public static void main(final String[] args) throws Exception {
        ServerLocator locator = ActiveMQClient.createServerLocator("tcp://127.0.0.1:61616");

        ClientSessionFactory factory = locator.createSessionFactory();
        ClientSession session = factory.createSession();

        QueueConfiguration queueConfiguration = new QueueConfiguration();
        queueConfiguration.setName("exampleCOUNT");
//        queueConfiguration.setAddress("queues/example");
        queueConfiguration.setRoutingType(RoutingType.ANYCAST);
        queueConfiguration.setDurable(false);
        session.createQueue(queueConfiguration);

        ClientProducer producer = session.createProducer("exampleCOUNT");
        ClientMessage message = session.createMessage(false);
        message.getBodyBuffer().writeString("Hello");

        long t0 = System.currentTimeMillis();
        for (int i = 0; i < COUNT_THRESHOLD; i++) {
            producer.send(message);
        }
        long t1 = System.currentTimeMillis();

        long send_elapsed = t1 - t0;
        System.out.println("Sending " + COUNT_THRESHOLD + " messages took " + send_elapsed + " ms (" + send_elapsed / 1000.0 +
                " s)");
        System.out.println("Throughput " + ((float) COUNT_THRESHOLD) / (send_elapsed / 1000.0) + " msg/s");

        // session.deleteQueue("exampleCOUNT");
        session.close();
    }
}
