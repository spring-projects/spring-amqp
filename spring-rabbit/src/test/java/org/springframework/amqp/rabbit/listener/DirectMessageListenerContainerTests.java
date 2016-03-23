/*
 * Copyright 2016 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import static org.junit.Assert.assertEquals;

import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * @author Gary Russell
 * @since 1.6
 *
 */
public class DirectMessageListenerContainerTests {

	@Rule
	public BrokerRunning brokerRunning = BrokerRunning.isRunningWithEmptyQueues("testQ1", "testQ2");

	@Test
	public void testSimple() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setThreadNamePrefix("client-");
		executor.afterPropertiesSet();
		cf.setExecutor(executor);
		SimpleDirectMessageListenerContainer container = new SimpleDirectMessageListenerContainer(cf);
		container.setQueueNames("testQ1", "testQ2");
		container.setConsumersPerQueue(2);
		container.setMessageListener(new MessageListenerAdapter(new Object() {

			@SuppressWarnings("unused")
			public String handleMessage(String in) {
				LogFactory.getLog(this.getClass()).info(in);
				if ("foo".equals(in) || "bar".equals(in)) {
					return in.toUpperCase();
				}
				else {
					return null;
				}
			}

		}));
		container.afterPropertiesSet();
		container.start();
		RabbitTemplate template = new RabbitTemplate(cf);
		assertEquals("FOO", template.convertSendAndReceive("testQ1", "foo"));
		assertEquals("BAR", template.convertSendAndReceive("testQ2", "bar"));
	}

}
