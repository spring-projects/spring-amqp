/*
 * Copyright 2014 the original author or authors.
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

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.BrokerRunning;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Gary Russell
 * @since 1.3
 *
 */
public class ListenFromAutoDeleteQueueTests {

	@Rule
	public BrokerRunning brokerIsRunning = BrokerRunning.isRunning();

	private SimpleMessageListenerContainer listenerContainer;

	private ConfigurableApplicationContext context;

	private static BlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();

	@Before
	public void setup() {
		this.context = new ClassPathXmlApplicationContext(this.getClass().getSimpleName() + "-context.xml",
				this.getClass());
		this.listenerContainer = context.getBean(SimpleMessageListenerContainer.class);
	}

	@After
	public void tearDown() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void testStopStart() throws Exception {
		RabbitTemplate template = context.getBean(RabbitTemplate.class);
		template.convertAndSend("testContainerWithAutoDeleteQueues", "anon", "foo");
		assertNotNull(queue.poll(10, TimeUnit.SECONDS));
		this.listenerContainer.stop();
		RabbitAdmin admin = spy(TestUtils.getPropertyValue(this.listenerContainer, "rabbitAdmin", RabbitAdmin.class));
		new DirectFieldAccessor(this.listenerContainer).setPropertyValue("rabbitAdmin", admin);
		this.listenerContainer.start();
		template.convertAndSend("testContainerWithAutoDeleteQueues", "anon", "foo");
		assertNotNull(queue.poll(10, TimeUnit.SECONDS));
		verify(admin, times(1)).initialize(); // should only be called by one of the consumers
	}

	public static class Listener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			queue.add(message);
		}

	}

}
