/*
 * Copyright 2002-2016 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.DirectFieldAccessor;

public final class SimpleMessageListenerWithRabbitMQ {

	private static Log logger = LogFactory.getLog(SimpleMessageListenerWithRabbitMQ.class);


	private SimpleMessageListenerWithRabbitMQ() {
		super();
	}


	public static void main(String[] args) throws InterruptedException {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setHost("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		assertNotNull(connectionFactory);

		MessageConverter messageConverter = new SimpleMessageConverter();
		MessageProperties  messageProperties = new MessageProperties();
		messageProperties.setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN);

		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory);
		container.setQueueNames("foo");
		container.setPrefetchCount(1000);
		container.setTxSize(500);
		container.setAcknowledgeMode(AcknowledgeMode.AUTO);
		container.setConcurrentConsumers(20);
		container.setMessageListener(new MessageListenerAdapter(new SimpleAdapter(), messageConverter));
		container.start();

		RabbitTemplate template = new RabbitTemplate(connectionFactory);
		template.setMessageConverter(messageConverter);
		List<BlockingQueue<?>> queues = getQueues(container);

		Thread.sleep(10000);
		int n = 0;
		while (true) {
			for (int i = 1; i <= 200; i++) {

				template.send("foo", "", new Message("foo # ID: id".replace("#", String.valueOf(i)).replace("id", java.util.UUID.randomUUID().toString()).getBytes(), messageProperties));

			}
			Thread.sleep(1000);
			if (++n % 10 == 0) {
				logger.warn(count(queues));
			}
		}
	}


	private static String count(List<BlockingQueue<?>> queues) {
		int n = 0;
		for (BlockingQueue<?> queue : queues) {
			n += queue.size();
		}
		return "Total queue size: " + n;
	}


	private static List<BlockingQueue<?>> getQueues(SimpleMessageListenerContainer container) {
		DirectFieldAccessor accessor = new DirectFieldAccessor(container);
		List<BlockingQueue<?>> queues = new ArrayList<BlockingQueue<?>>();
		@SuppressWarnings("unchecked")
		Set<BlockingQueueConsumer> consumers = (Set<BlockingQueueConsumer>) accessor.getPropertyValue("consumers");
		for (BlockingQueueConsumer consumer : consumers) {
			accessor = new DirectFieldAccessor(consumer);
			queues.add((BlockingQueue<?>) accessor.getPropertyValue("queue"));
		}
		return queues;
	}



	private static class SimpleAdapter {

		SimpleAdapter() {
			super();
		}

		@SuppressWarnings("unused")
		public void handleMessage(String input) {
			logger.debug("Got it: " + input);
		}
	}

}
