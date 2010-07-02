/*
 * Copyright 2002-2010 the original author or authors.
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

package org.springframework.amqp.rabbit.admin;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

/**
 * @author Mark Fisher
 * @author Mark Pollack
 */
@SuppressWarnings("unused")
public class RabbitTemplateConsumerExample {

	private static Log log = LogFactory
			.getLog(RabbitTemplateConsumerExample.class);

	public static void main(String[] args) throws Exception {

		boolean sync = true;
		if (sync) {
			ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(
					TestRabbitConfiguration.class);
			RabbitTemplate template = ctx.getBean(RabbitTemplate.class);
			receiveSync(template, TestConstants.NUM_MESSAGES);
		} else {
			ConfigurableApplicationContext ctx = new AnnotationConfigApplicationContext(
					RabbitConsumerConfiguration.class);
			receiveAsync(ctx);
		}

	}

	private static void receiveAsync(ConfigurableApplicationContext ctx)
			throws InterruptedException {

		SimpleMessageListenerContainer container = ctx
				.getBean(SimpleMessageListenerContainer.class);

		log.debug("Main execution thread sleeping 5 seconds...");
		Thread.sleep(500000);
		log.debug("Application exiting.");

		System.exit(0);

	}

	private static void receiveSync(RabbitTemplate template, int numMessages) {
		// receive response
		for (int i = 0; i < numMessages; i++) {
			Message message = template.receive(TestConstants.QUEUE_NAME);
			if (message == null) {
				System.out.println("Thread [" + Thread.currentThread().getId()
						+ "] Received Null Message!");
			} else {
				System.out.println("Thread [" + Thread.currentThread().getId()
						+ "] Received Message = "
						+ new String(message.getBody()));
				Map<String, Object> headers = message.getMessageProperties()
						.getHeaders();

				Object objFloat = headers.get("float");
				Object objcp = headers.get("object");
				System.out.println("float header type = " + objFloat.getClass());
				System.out.println("object header type = " + objcp.getClass());
			}
		}
	}

	public static class SimpleMessageListener implements MessageListener {

		private final AtomicInteger messageCount = new AtomicInteger();

		public void onMessage(Message message) {
			int msgCount = this.messageCount.incrementAndGet();
			System.out.println("Thread [" + Thread.currentThread().getId()
					+ "] SimpleMessageListener Received Message " + msgCount
					+ ", = " + new String(message.getBody()));
		}
	}
}
