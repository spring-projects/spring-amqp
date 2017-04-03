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

package org.springframework.amqp.rabbit.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.MessagingMessageListenerAdapter;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * A {@link RabbitTemplate} that invokes {@code @RabbitListener} s directly.
 * It currently only supports the queue name in the routing key.
 * It does not currently support publisher confirms/returns.
 *
 * @author Gary Russell
 * @since 2.0
 *
 */
public class TestRabbitTemplate extends RabbitTemplate implements SmartInitializingSingleton {

	private static final String REPLY_QUEUE = "testRabbitTemplateReplyTo";

	private final Map<String, MessagingMessageListenerAdapter> listeners = new HashMap<>();

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	public TestRabbitTemplate(ConnectionFactory connectionFactory) {
		super(connectionFactory);
		setReplyAddress(REPLY_QUEUE);
	}

	@Override
	public void afterSingletonsInstantiated() {
		registry.getListenerContainers()
			.stream()
			.map(c -> (AbstractMessageListenerContainer) c)
			.forEach(c -> {
				for (String queue : c.getQueueNames()) {
					listeners.put(queue, (MessagingMessageListenerAdapter) c.getMessageListener());
				}
				// TODO: support multiple listeners for the same queue(s)? That's a bit odd with @RabbitListener though.
			});
	}

	@Override
	protected boolean useDirectReplyTo() {
		return false;
	}

	@Override
	protected void sendToRabbit(Channel channel, String exchange, String routingKey, boolean mandatory,
			Message messageToUse) throws IOException {
		MessagingMessageListenerAdapter adapter = listeners.get(routingKey);
		if (adapter == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		try {
			adapter.onMessage(messageToUse, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	protected Message doSendAndReceiveWithFixed(String exchange, String routingKey, Message message,
			CorrelationData correlationData) {
		MessagingMessageListenerAdapter adapter = listeners.get(routingKey);
		if (adapter == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		Channel channel = mock(Channel.class);
		AtomicReference<Message> reply = new AtomicReference<>();
		try {
			willAnswer(i -> {
				Envelope envelope = new Envelope(1, false, "", REPLY_QUEUE);
				reply.set(MessageBuilder.withBody(i.getArgument(4))
						.andProperties(getMessagePropertiesConverter().toMessageProperties(i.getArgument(3), envelope,
								adapter.getEncoding()))
						.build());
				return null;
			}).given(channel).basicPublish(anyString(), anyString(), anyBoolean(), any(BasicProperties.class),
					any(byte[].class));
			message.getMessageProperties().setReplyTo(REPLY_QUEUE);
			adapter.onMessage(message, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
		return reply.get();
	}

}
