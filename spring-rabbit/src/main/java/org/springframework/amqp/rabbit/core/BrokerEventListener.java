/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Base64UrlNamingStrategy;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.DirectMessageListenerContainer;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * When the event-exchange-plugin is enabled (see
 * https://www.rabbitmq.com/event-exchange.html), if an object of this type is declared as
 * a bean, selected events will be published as {@link BrokerEvent}s. Such events can then
 * be consumed using an {@code ApplicationListener} or {@code @EventListener} method.
 * An {@link AnonymousQueue} will be bound to the {@code amq.rabbitmq.event} topic exchange
 * with the supplied keys.
 *
 * @author Gary Russell
 * @since 2.1
 *
 */
public class BrokerEventListener implements MessageListener, ApplicationEventPublisherAware, ConnectionListener,
		SmartLifecycle {

	private static final Log logger = LogFactory.getLog(BrokerEventListener.class); // NOSONAR - lower case

	private final AbstractMessageListenerContainer container;

	private final String[] eventKeys;

	private final RabbitAdmin admin;

	private final Queue eventQueue = new AnonymousQueue(new Base64UrlNamingStrategy("spring.events."));

	private final boolean ownContainer;

	private int phase;

	private boolean autoStartup = true;

	private boolean running;

	private boolean stopInvoked;

	private Exception bindingsFailedException;

	private ApplicationEventPublisher applicationEventPublisher;

	/**
	 * Construct an instance using the supplied connection factory and event keys. Event
	 * keys are patterns to match routing keys for events published to the
	 * {@code amq.rabbitmq.event} topic exchange. They can therefore match wildcards;
	 * examples are {@code user.#, queue.created}. Refer to the plugin documentation for
	 * information about available events. A single-threaded
	 * {@link DirectMessageListenerContainer} will be created; its lifecycle will be
	 * controlled by this object's {@link SmartLifecycle} methods.
	 * @param connectionFactory the connection factory.
	 * @param eventKeys the event keys.
	 */
	public BrokerEventListener(ConnectionFactory connectionFactory, String... eventKeys) {
		this(new DirectMessageListenerContainer(connectionFactory), true, eventKeys);
	}

	/**
	 * Construct an instance using the supplied listener container factory and event keys.
	 * Event keys are patterns to match routing keys for events published to the
	 * {@code amq.rabbitmq.event} topic exchange. They can therefore match wildcards;
	 * examples are {@code user.#, queue.created}. Refer to the plugin documentation for
	 * information about available events. The container's lifecycle will be not be
	 * controlled by this object's {@link SmartLifecycle} methods. The container should
	 * not be configured with queues or a {@link MessageListener}; those properties will
	 * be replaced.
	 * @param container the listener container.
	 * @param eventKeys the event keys.
	 */
	public BrokerEventListener(AbstractMessageListenerContainer container, String... eventKeys) {
		this(container, false, eventKeys);
	}

	private BrokerEventListener(AbstractMessageListenerContainer container, boolean ownContainer, String... eventKeys) {
		Assert.notNull(container, "listener container cannot be null");
		Assert.isTrue(!ObjectUtils.isEmpty(eventKeys), "At least one event key is required");
		this.container = container;
		this.container.setQueues(this.eventQueue);
		this.container.setMessageListener(this);
		this.eventKeys = Arrays.copyOf(eventKeys, eventKeys.length);
		this.container.getConnectionFactory().addConnectionListener(this);
		this.admin = new RabbitAdmin(this.container.getConnectionFactory());
		this.ownContainer = ownContainer;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	/**
	 * Return any exception thrown when attempting to bind the queue to the event exchange.
	 * @return the exception.
	 */
	public @Nullable Exception getBindingsFailedException() {
		return this.bindingsFailedException;
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
			if (this.stopInvoked) {
				// redeclare auto-delete queue
				this.stopInvoked = false;
				onCreate(null);
			}
			if (this.ownContainer) {
				this.container.start();
			}
			this.running = true;
		}
	}

	@Override
	public synchronized void stop() {
		if (this.running) {
			if (this.ownContainer) {
				this.container.stop();
			}
			this.running = false;
			this.stopInvoked = true;
		}
	}

	@Override
	public synchronized boolean isRunning() {
		return this.running;
	}

	@Override
	public int getPhase() {
		return this.phase;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void onMessage(Message message) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(new BrokerEvent(this, message.getMessageProperties()));
		}
		else {
			if (logger.isWarnEnabled()) {
				logger.warn("No event publisher available for " + message + "; if the BrokerEventListener "
						+ "is not defined as a bean, you must provide an ApplicationEventPublisher");
			}
		}
	}

	@Override
	public void onCreate(@Nullable Connection connection) {
		this.bindingsFailedException = null;
		TopicExchange exchange = new TopicExchange("amq.rabbitmq.event");
		try {
			this.admin.declareQueue(this.eventQueue);
			Arrays.stream(this.eventKeys).forEach(k -> {
				Binding binding = BindingBuilder.bind(this.eventQueue).to(exchange).with(k);
				this.admin.declareBinding(binding);
			});
		}
		catch (Exception e) {
			logger.error("failed to declare event queue/bindings - is the plugin enabled?", e);
			this.bindingsFailedException = e;
		}

	}

}
