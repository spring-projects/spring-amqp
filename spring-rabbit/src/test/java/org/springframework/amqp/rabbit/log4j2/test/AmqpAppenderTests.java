/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.amqp.rabbit.log4j2.test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.FanoutExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.BrokerRunning;
import org.springframework.amqp.rabbit.log4j2.AmqpAppender;
import org.springframework.amqp.utils.test.TestUtils;

/**
 * @author Gary Russell
 * @author Nicolas Ristock
 *
 * @since 1.6
 */
public class AmqpAppenderTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunning();

	@Test
	public void test() {
		CachingConnectionFactory ccf = new CachingConnectionFactory("localhost");
		RabbitTemplate template = new RabbitTemplate(ccf);
		RabbitAdmin admin = new RabbitAdmin(ccf);
		FanoutExchange fanout = new FanoutExchange("log4j2Test");
		admin.declareExchange(fanout);
		Queue queue = new Queue("log4jTest");
		admin.declareQueue(queue);
		admin.declareBinding(BindingBuilder.bind(queue).to(fanout));
		Logger logger = LogManager.getLogger("foo");
		logger.info("foo");
		template.setReceiveTimeout(10000);
		Message received = template.receive(queue.getName());
		assertNotNull(received);
		assertEquals("testAppId.foo.INFO", received.getMessageProperties().getReceivedRoutingKey());
		Object threadName = received.getMessageProperties().getHeaders().get("thread");
		assertNotNull(threadName);
		assertThat(threadName, instanceOf(String.class));
		assertThat(threadName, is(Thread.currentThread().getName()));
	}

	@Test
	public void testProperties() {
		Logger logger = LogManager.getLogger("foo");
		AmqpAppender appender = (AmqpAppender) TestUtils
				.getPropertyValue(logger, "context.configuration.appenders", Map.class).get("rabbitmq");
		Object manager = TestUtils.getPropertyValue(appender, "manager");
//		<RabbitMQ name="rabbitmq"
//				addresses="localhost:5672"
//				host="localhost" port="5672" user="guest" password="guest" virtualHost="/"
//				exchange="log4j2Test" exchangeType="fanout" declareExchange="true" durable="true" autoDelete="false"
//				applicationId="testAppId" routingKeyPattern="%X{applicationId}.%c.%p"
//				contentType="text/plain" contentEncoding="UTF-8" generateId="true" deliveryMode="NON_PERSISTENT"
//				charset="UTF-8"
//				senderPoolSize="3" maxSenderRetries="5">
//		</RabbitMQ>
		assertEquals("localhost:5672", TestUtils.getPropertyValue(manager, "addresses"));
		assertEquals("localhost", TestUtils.getPropertyValue(manager, "host"));
		assertEquals(5672, TestUtils.getPropertyValue(manager, "port"));
		assertEquals("guest", TestUtils.getPropertyValue(manager, "username"));
		assertEquals("guest", TestUtils.getPropertyValue(manager, "password"));
		assertEquals("/", TestUtils.getPropertyValue(manager, "virtualHost"));
		assertEquals("log4j2Test", TestUtils.getPropertyValue(manager, "exchangeName"));
		assertEquals("fanout", TestUtils.getPropertyValue(manager, "exchangeType"));
		assertTrue(TestUtils.getPropertyValue(manager, "declareExchange", Boolean.class));
		assertTrue(TestUtils.getPropertyValue(manager, "durable", Boolean.class));
		assertFalse(TestUtils.getPropertyValue(manager, "autoDelete", Boolean.class));
		assertEquals("testAppId", TestUtils.getPropertyValue(manager, "applicationId"));
		assertEquals("%X{applicationId}.%c.%p", TestUtils.getPropertyValue(manager, "routingKeyPattern"));
		assertEquals("text/plain", TestUtils.getPropertyValue(manager, "contentType"));
		assertEquals("UTF-8", TestUtils.getPropertyValue(manager, "contentEncoding"));
		assertTrue(TestUtils.getPropertyValue(manager, "generateId", Boolean.class));
		assertEquals(MessageDeliveryMode.NON_PERSISTENT, TestUtils.getPropertyValue(manager, "deliveryMode"));
		assertEquals("UTF-8", TestUtils.getPropertyValue(manager, "contentEncoding"));
		assertEquals(3, TestUtils.getPropertyValue(manager, "senderPoolSize"));
		assertEquals(5, TestUtils.getPropertyValue(manager, "maxSenderRetries"));
	}

}
