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

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

/**
 * RabbitMQ implementation of portable AMQP administrative operations for AMQP >= 0.9.1
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class RabbitAdmin implements AmqpAdmin, ApplicationContextAware, SmartLifecycle {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private final RabbitTemplate rabbitTemplate;

	private volatile boolean running;

	private volatile boolean autoStartup = true;

	private volatile int phase = Integer.MIN_VALUE;

	private volatile ApplicationContext applicationContext;

	private final Object lifecycleMonitor = new Object();


	public RabbitAdmin(ConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
	}

	public RabbitAdmin(RabbitTemplate rabbitTemplate) {
		Assert.notNull(rabbitTemplate, "RabbitTemplate must not be null");
		this.rabbitTemplate = rabbitTemplate;
	}


	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	public void setPhase(int phase) {
		this.phase = phase;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	public RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}

	// Exchange operations

	public void declareExchange(final Exchange exchange) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				declareExchanges(channel, exchange);
				return null;
			}
		});
	}

	@ManagedOperation
	public void deleteExchange(final String exchangeName) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.exchangeDelete(exchangeName);
				return null;
			}
		});
	}

	// Queue operations

	@ManagedOperation
	public void declareQueue(final Queue queue) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				declareQueues(channel, queue);
				return null;
			}
		});
	}

	/**
	 * Declares a server-named exclusive, autodelete, non-durable queue. 
	 */
	@ManagedOperation
	public Queue declareQueue() {
		DeclareOk declareOk = this.rabbitTemplate.execute(new ChannelCallback<DeclareOk>() {
			public DeclareOk doInRabbit(Channel channel) throws Exception {
				return channel.queueDeclare();
			}
		});			
		Queue queue = new Queue(declareOk.getQueue());
		queue.setExclusive(true);
		queue.setAutoDelete(true);
		queue.setDurable(false);
		return queue;
	}

	@ManagedOperation
	public void deleteQueue(final String queueName) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDelete(queueName);
				return null;
			}
		});
	}

	@ManagedOperation
	public void deleteQueue(final String queueName, final boolean unused, final boolean empty) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDelete(queueName, unused, empty);
				return null;
			}
		});
	}

	@ManagedOperation
	public void purgeQueue(final String queueName, final boolean noWait) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queuePurge(queueName);
				return null;
			}
		});
	}

	// Binding
	@ManagedOperation
	public void declareBinding(final Binding binding) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				declareBindings(channel, binding);
				return null;
			}
		});
	}


	// Lifecycle implementation

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public int getPhase() {
		return this.phase;
	}

	public boolean isRunning() {
		return this.running;
	}

	public void start() {
		synchronized (this.lifecycleMonitor) {
			if (this.running) {
				return;
			}
			if (this.applicationContext == null) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("no ApplicationContext has been set, cannot auto-declare Exchanges, Queues, and Bindings");
				}
				return;
			}
			final Collection<Exchange> exchanges = this.applicationContext.getBeansOfType(Exchange.class).values();
			final Collection<Queue> queues = this.applicationContext.getBeansOfType(Queue.class).values();
			final Collection<Binding> bindings = this.applicationContext.getBeansOfType(Binding.class).values();
			this.rabbitTemplate.execute(new ChannelCallback<Object>() {
				public Object doInRabbit(Channel channel) throws Exception {
					declareExchanges(channel, exchanges.toArray(new Exchange[exchanges.size()]));
					declareQueues(channel, queues.toArray(new Queue[queues.size()]));
					declareBindings(channel, bindings.toArray(new Binding[bindings.size()]));
					return null;
				}
			});
			this.running = true;
		}
	}

	public void stop() {
		this.running = false;
	}

	public void stop(Runnable callback) {
		this.stop();
		callback.run();
	}


	// private methods for declaring Exchanges, Queues, and Bindings on a Channel

	private void declareExchanges(final Channel channel, final Exchange... exchanges) throws IOException {
		for (final Exchange exchange : exchanges) {
			if (logger.isDebugEnabled()) {
				logger.debug("declaring Exchange '" + exchange.getName() + "'");
			}
			channel.exchangeDeclare(exchange.getName(), exchange.getType(),
					exchange.isDurable(), exchange.isAutoDelete(), exchange.getArguments());
		}
	}

	private void declareQueues(final Channel channel, final Queue... queues) throws IOException {
		for (Queue queue : queues) {
			if (!queue.getName().startsWith("amq.")) {
				if (logger.isDebugEnabled()) {
					logger.debug("declaring Queue '" + queue.getName() + "'");
				}
				channel.queueDeclare(queue.getName(), queue.isDurable(),
						queue.isExclusive(), queue.isAutoDelete(), queue.getArguments());
			}
			else if (logger.isDebugEnabled()) {
				logger.debug("Queue with name that starts with 'amq.' cannot be declared.");
			}
		}
	}


	private void declareBindings(final Channel channel, final Binding... bindings) throws IOException {
		for (Binding binding : bindings) {
			if (logger.isDebugEnabled()) {
				logger.debug("Binding queue [" + binding.getQueue() + "] to exchange [" +
						binding.getExchange() + "] with routing key [" + binding.getRoutingKey() + "]");
			}
			channel.queueBind(binding.getQueue(), binding.getExchange(),
					binding.getRoutingKey(), binding.getArguments());
		}
	}

}
