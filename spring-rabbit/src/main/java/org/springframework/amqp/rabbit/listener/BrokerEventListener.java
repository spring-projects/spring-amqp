/*
 * Copyright 2018 the original author or authors.
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
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Gary Russell
 * @since 2.1
 *
 */
public class BrokerEventListener implements MessageListener, ApplicationEventPublisherAware, ConnectionListener,
		SmartLifecycle {

	private static final Log logger = LogFactory.getLog(BrokerEventListener.class);

	private final AbstractMessageListenerContainer container;

	private final String[] eventKeys;

	private final RabbitAdmin admin;

	private final Queue eventQueue = new AnonymousQueue(new Base64UrlNamingStrategy("spring.events."));

	private final boolean ownContainer;

	private int phase;

	private boolean autoStartup = true;

	private volatile boolean running;

	private Exception bindingsFailedException;

	private ApplicationEventPublisher applicationEventPublisher;

	public BrokerEventListener(ConnectionFactory connectionFactory, String... eventKeys) {
		this(new DirectMessageListenerContainer(connectionFactory), true, eventKeys);
	}

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

	public Exception getBindingsFailedException() {
		return this.bindingsFailedException;
	}

	@Override
	public synchronized void start() {
		if (!this.running) {
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
		}
	}

	@Override
	public boolean isRunning() {
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
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public void onMessage(Message message) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(new BrokerEvent(this, message.getMessageProperties()));
		}
	}

	@Override
	public void onCreate(Connection connection) {
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
