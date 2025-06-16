/*
 * Copyright 2014-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 1.3
 */
@RabbitAvailable
public class ListenFromAutoDeleteQueueTests {

	private SimpleMessageListenerContainer listenerContainer1;

	private SimpleMessageListenerContainer listenerContainer2;

	private ConfigurableApplicationContext context;

	private static BlockingQueue<Message> queue = new LinkedBlockingQueue<Message>();

	private Queue expiringQueue;

	@BeforeEach
	public void setup() {
		this.context = new ClassPathXmlApplicationContext(this.getClass().getSimpleName() + "-context.xml",
				this.getClass());
		this.listenerContainer1 = context.getBean("container1", SimpleMessageListenerContainer.class);
		this.listenerContainer2 = context.getBean("container2", SimpleMessageListenerContainer.class);
		this.expiringQueue = context.getBean("xExpires", Queue.class);
	}

	@AfterEach
	public void tearDown() {
		if (this.context != null) {
			this.context.close();
		}
	}

	@Test
	public void testStopStart() throws Exception {
		RabbitTemplate template = context.getBean(RabbitTemplate.class);
		template.convertAndSend("testContainerWithAutoDeleteQueues", "anon", "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		this.listenerContainer1.stop();
		RabbitAdmin admin = spy(TestUtils.getPropertyValue(this.listenerContainer1, "amqpAdmin", RabbitAdmin.class));
		new DirectFieldAccessor(this.listenerContainer1).setPropertyValue("amqpAdmin", admin);
		this.listenerContainer1.start();
		template.convertAndSend("testContainerWithAutoDeleteQueues", "anon", "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		verify(admin, times(1)).initialize(); // should only be called by one of the consumers
	}

	@Test
	public void testStopStartConditionalDeclarations() throws Exception {
		RabbitTemplate template = context.getBean(RabbitTemplate.class);
		this.listenerContainer2.start();
		template.convertAndSend("otherExchange", "otherAnon", "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		this.listenerContainer2.stop();
		this.listenerContainer2.start();
		template.convertAndSend("otherExchange", "otherAnon", "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
	}

	@Test
	public void testRedeclareXExpiresQueue() throws Exception {
		RabbitTemplate template = context.getBean(RabbitTemplate.class);
		template.convertAndSend(this.expiringQueue.getName(), "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		SimpleMessageListenerContainer listenerContainer = context.getBean("container3",
				SimpleMessageListenerContainer.class);
		listenerContainer.stop();
		RabbitAdmin admin = spy(TestUtils.getPropertyValue(listenerContainer, "amqpAdmin", RabbitAdmin.class));
		new DirectFieldAccessor(listenerContainer).setPropertyValue("amqpAdmin", admin);
		await().until(() -> admin.getQueueProperties(this.expiringQueue.getName()) == null);
		listenerContainer.start();
		template.convertAndSend(this.expiringQueue.getName(), "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		verify(admin, atLeastOnce()).initialize(); // with short x-expires, both consumers might redeclare
	}

	@Test
	public void testAutoDeclareFalse() throws Exception {
		RabbitTemplate template = context.getBean(RabbitTemplate.class);
		template.convertAndSend("testContainerWithAutoDeleteQueues", "anon2", "foo");
		assertThat(queue.poll(10, TimeUnit.SECONDS)).isNotNull();
		SimpleMessageListenerContainer listenerContainer = context.getBean("container4",
				SimpleMessageListenerContainer.class);
		listenerContainer.stop();
		RabbitAdmin admin = spy(TestUtils.getPropertyValue(listenerContainer, "amqpAdmin", RabbitAdmin.class));
		new DirectFieldAccessor(listenerContainer).setPropertyValue("amqpAdmin", admin);
		listenerContainer = spy(listenerContainer);

		//Prevent a long 'passiveDeclare' process
		BlockingQueueConsumer consumer = mock(BlockingQueueConsumer.class);
		willThrow(RuntimeException.class).given(consumer).start();
//		given(consumer.getBackOffExecution()).willReturn(mock(BackOffExecution.class));
		given(listenerContainer.createBlockingQueueConsumer()).willReturn(consumer);

		listenerContainer.start();
		listenerContainer.stop();
		verify(admin, never()).initialize(); // should not be called since 'autoDeclare = false'
	}

	public static class Listener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			queue.add(message);
		}

	}

}
