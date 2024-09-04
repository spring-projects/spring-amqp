/*
 * Copyright 2017-2024 the original author or authors.
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

package org.springframework.amqp.rabbit.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.amqp.rabbit.listener.adapter.AbstractAdaptableMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;

/**
 * A {@link RabbitTemplate} that invokes {@code @RabbitListener} s directly.
 * It currently only supports the queue name in the routing key.
 * It does not currently support publisher confirms/returns.
 *
 * @author Gary Russell
 * @author Artem Bilan
 * @author Christian Tzolov
 *
 * @since 2.0
 *
 */
public class TestRabbitTemplate extends RabbitTemplate
		implements ApplicationContextAware, ApplicationListener<ContextRefreshedEvent> {

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
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (event.getApplicationContext().equals(this.applicationContext)) {
			Stream<AbstractMessageListenerContainer> registryListenerContainers =
					this.registry.getListenerContainers()
							.stream()
							.map(AbstractMessageListenerContainer.class::cast);

			Stream<AbstractMessageListenerContainer> listenerContainerBeans =
					this.applicationContext.getBeansOfType(AbstractMessageListenerContainer.class).values().stream();

			Stream.concat(registryListenerContainers, listenerContainerBeans)
					.forEach(container ->
							Arrays.stream(container.getQueueNames())
									.forEach(queue -> setupListener(container, queue)));
		}
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

		Listeners listenersForRoute = this.listeners.get(routingKey);
		if (listenersForRoute == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		try {
			invoke(listenersForRoute.next(), message, channel);
		}
		catch (Exception e) {
			throw RabbitExceptionTranslator.convertRabbitAccessException(e);
		}
	}

	@Override
	protected Message doSendAndReceiveWithFixed(String exchange, String routingKey, Message message,
			CorrelationData correlationData) {

		Listeners listenersForRoute = this.listeners.get(routingKey);
		if (listenersForRoute == null) {
			throw new IllegalArgumentException("No listener for " + routingKey);
		}
		Channel channel = mock(Channel.class);
		final AtomicReference<Message> reply = new AtomicReference<>();
		Object listener = listenersForRoute.next();
		if (listener instanceof AbstractAdaptableMessageListener adapter) {
			try {
				willAnswer(i -> {
					Envelope envelope = new Envelope(1, false, "", REPLY_QUEUE);
					reply.set(MessageBuilder.withBody(i.getArgument(4)) // NOSONAR magic #
							.andProperties(getMessagePropertiesConverter()
									.toMessageProperties(i.getArgument(3), envelope, // NOSONAR magic #
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
		if (listener instanceof ChannelAwareMessageListener channelAwareMessageListener) {
			try {
				channelAwareMessageListener.onMessage(message, channel);
			}
			catch (Exception e) {
				throw RabbitExceptionTranslator.convertRabbitAccessException(e);
			}
		}
		else if (listener instanceof MessageListener messageListener) {
			messageListener.onMessage(message);
		}
		else {
			// Not really necessary since the container doesn't allow it, but no hurt
			throw new IllegalStateException("Listener of type " + listener.getClass().getName() + " is not supported");
		}
	}

	private static class Listeners {

		private final Lock lock = new ReentrantLock();

		private final List<Object> listeners = new ArrayList<>();

		private volatile Iterator<Object> iterator;

		Listeners() {
		}

		private Object next() {
			this.lock.lock();
			try {
				if (this.iterator == null || !this.iterator.hasNext()) {
					this.iterator = this.listeners.iterator();
				}
				return this.iterator.next();
			}
			finally {
				this.lock.unlock();
			}
		}

	}

}
