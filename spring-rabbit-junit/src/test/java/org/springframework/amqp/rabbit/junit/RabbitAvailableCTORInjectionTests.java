/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.junit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 2.0.2
 *
 */
@RabbitAvailable(queues = "rabbitAvailableTests.queue")
public class RabbitAvailableCTORInjectionTests {

	private final ConnectionFactory connectionFactory;

	public RabbitAvailableCTORInjectionTests(BrokerRunning brokerRunning) {
		this.connectionFactory = brokerRunning.getConnectionFactory();
	}

	@Test
	public void test(ConnectionFactory cf) throws Exception {
		assertSame(cf, this.connectionFactory);
		Connection conn = this.connectionFactory.newConnection();
		Channel channel = conn.createChannel();
		DeclareOk declareOk = channel.queueDeclarePassive("rabbitAvailableTests.queue");
		assertEquals(0, declareOk.getConsumerCount());
		channel.close();
		conn.close();
	}

}
