/*
 * Copyright 2017-2018 the original author or authors.
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * A {@link RabbitTemplate} that invokes {@code @RabbitListener} s directly.
 * It currently only supports the queue name in the routing key.
 * It does not currently support publisher confirms/returns.
 *
 * @author Gary Russell
 *
 * @since 2.0
 *
 */
public class TestRabbitTemplate extends RabbitTemplate implements ApplicationContextAware, SmartInitializingSingleton {

	private static final String REPLY_QUEUE = "testRabbitTemplateReplyTo";

	private final Map<String, Listeners> listeners = new HashMap<>();

	private ApplicationContext applicationContext;

	@Autowired
	private RabbitListenerEndpointRegistry registry;

	public TestRabbitTemplate(ConnectionFactory connectionFactory) {
		super(connectionFactory);
		setReplyAddress(REPLY_QUEUE);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void afterSingletonsInstantiated() {
		this.registry.getListenerContainers()
			.stream()
			.map(container -> (AbstractMessageListenerContainer) container)
			.forEach(c -> {
				for (String queue : c.getQueueNames()) {
					setupListener(c, queue);
				}
			});
		this.applicationContext.getBeansOfType(AbstractMessageListenerContainer.class).values()
			.stream()
			.forEach(container -> {
				for (String queue : container.getQueueNames()) {
					setupListener(container, queue);
				}
			});
	}

	private void setupListener(AbstractMessageListenerContainer container, String queue) {
		this.listeners.computeIfAbsent(queue, v -> new Listeners()).listeners.add(container.getMessageListener());
	}

	@Override
	protected boolean useDirectReplyTo() {
		return false;
	}

	@Override
	protected void sendToRabbit(Channel channel, String exchange, String routingKey, boolean mandatory,
			Message message) {

		Listeners listeners = this.listeners.get(routingKey);
		if (listeners == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		try {
			invoke(listeners.next(), message, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	protected Message doSendAndReceiveWithFixed(String exchange, String routingKey, Message message,
			CorrelationData correlationData) {
		Listeners listeners = this.listeners.get(routingKey);
		if (listeners == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		Channel channel = mock(Channel.class);
		final AtomicReference<Message> reply = new AtomicReference<>();
		Object listener = listeners.next();
		if (listener instanceof AbstractAdaptableMessageListener) {
			try {
				AbstractAdaptableMessageListener adapter = (AbstractAdaptableMessageListener) listener;
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
		}
		else {
			throw new IllegalStateException("sendAndReceive not supported for " + listener.getClass().getName());
		}
		return reply.get();
	}

	private void invoke(Object listener, Message message, Channel channel) {
		if (listener instanceof ChannelAwareMessageListener) {
			try {
				((ChannelAwareMessageListener) listener).onMessage(message, channel);
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else if (listener instanceof MessageListener) {
			((MessageListener) listener).onMessage(message);
		}
		else {
			// Not really necessary since the container doesn't allow it, but no hurt
			throw new IllegalStateException("Listener of type " + listener.getClass().getName() + " is not supported");
		}
	}

	private static class Listeners {

		private final List<Object> listeners = new ArrayList<>();

		private volatile Iterator<Object> iterator;

		Listeners() {
			super();
		}

		private synchronized Object next() {
			if (this.iterator == null || !this.iterator.hasNext()) {
				this.iterator = this.listeners.iterator();
			}
			return this.iterator.next();
		}

	}

}
