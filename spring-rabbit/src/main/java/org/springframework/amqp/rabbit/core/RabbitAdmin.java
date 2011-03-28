/*
 * Copyright 2002-2010 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

/**
 * RabbitMQ implementation of portable AMQP administrative operations for AMQP >= 0.9.1
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 */
public class RabbitAdmin implements AmqpAdmin, ApplicationContextAware, InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private final RabbitTemplate rabbitTemplate;

	private volatile boolean running = false;

	private volatile boolean autoStartup = true;

	private volatile ApplicationContext applicationContext;

	private final Object lifecycleMonitor = new Object();

	private final ConnectionFactory connectionFactory;

	public RabbitAdmin(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
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
	public boolean deleteExchange(final String exchangeName) {
		return this.rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			public Boolean doInRabbit(Channel channel) throws Exception {
				try {
					channel.exchangeDelete(exchangeName);
				} catch (IOException e) {
					return false;
				}
				return true;
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
		Queue queue = new Queue(declareOk.getQueue(), true, true, false);
		return queue;
	}

	@ManagedOperation
	public boolean deleteQueue(final String queueName) {
		return this.rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			public Boolean doInRabbit(Channel channel) throws Exception {
				try {
					channel.queueDelete(queueName);
				} catch (IOException e) {
					return false;
				}
				return true;
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

	@ManagedOperation
	public void removeBinding(final Binding binding) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueUnbind(binding.getQueue(), binding.getExchange(), binding.getRoutingKey(),
						binding.getArguments());
				return null;
			}
		});
	}

	// Lifecycle implementation

	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * If {@link #setAutoStartup(boolean) autoStartup} is set to true, registers a callback on the
	 * {@link ConnectionFactory} to declare all exchanges and queues in the enclosing application context. If the
	 * callback fails then it may cause other clients of the connection factory to fail, but since only exchanges,
	 * queues and bindings are declared failure is not expected.
	 * 
	 * @see InitializingBean#afterPropertiesSet()
	 * @see #initialize()
	 */
	public void afterPropertiesSet() {

		synchronized (this.lifecycleMonitor) {

			if (this.running || !this.autoStartup) {
				return;
			}

			connectionFactory.addConnectionListener(new ConnectionListener() {

				// Prevent stack overflow...
				private AtomicBoolean initializing = new AtomicBoolean(false);

				public void onCreate(Connection connection) {
					if (!initializing.compareAndSet(false, true)) {
						// If we are already initializing, we don't need to do it again...
						return;
					}
					try {
						/*
						 * ...but it is possible for this to happen twice in the same ConnectionFactory (if more than
						 * one concurrent Connection is allowed). It's idempotent, so no big deal (a bit of network
						 * chatter). In fact it might even be a good thing: exclusive queues only make sense if they are
						 * declared for every connection. If anyone has a problem with it: use auto-startup="false".
						 */
						initialize();
					} finally {
						initializing.compareAndSet(true, false);
					}
				}

				public void onClose(Connection connection) {
				}

			});

			this.running = true;

		}
	}

	/**
	 * Declares all the exchanges, queues and bindings in the enclosing application context, if any. It should be safe
	 * (but unnecessary) to call this method more than once.
	 */
	public void initialize() {

		if (this.applicationContext == null) {
			if (this.logger.isDebugEnabled()) {
				this.logger
						.debug("no ApplicationContext has been set, cannot auto-declare Exchanges, Queues, and Bindings");
			}
			return;
		}

		logger.debug("Initializing declarations");
		final Collection<Exchange> exchanges = applicationContext.getBeansOfType(Exchange.class).values();
		final Collection<Queue> queues = applicationContext.getBeansOfType(Queue.class).values();
		final Collection<Binding> bindings = applicationContext.getBeansOfType(Binding.class).values();

		for (Exchange exchange : exchanges) {
			if (!exchange.isDurable()) {
				logger.warn("Auto-declaring a non-durable Exchange ("
						+ exchange.getName()
						+ "). It will be deleted by the broker if it shuts down, and can be redeclared by closing and reopening the connection.");
			}
			if (exchange.isAutoDelete()) {
				logger.warn("Auto-declaring an auto-delete Exchange ("
						+ exchange.getName()
						+ "). It will be deleted by the broker if not in use (if all bindings are deleted), but will only be redeclared if the connection is closed and reopened.");
			}
		}

		for (Queue queue : queues) {
			if (!queue.isDurable()) {
				logger.warn("Auto-declaring a non-durable Queue ("
						+ queue.getName()
						+ "). It will be redeclared if the broker stops and is restarted while the connection factory is alive, but all messages will be lost.");
			}
			if (queue.isAutoDelete()) {
				logger.warn("Auto-declaring an auto-delete Queue ("
						+ queue.getName()
						+ "). It will be deleted deleted by the broker if not in use, and all messages will be lost.  Redeclared when the connection is closed and reopened.");
			}
			if (queue.isExclusive()) {
				logger.warn("Auto-declaring an exclusive Queue ("
						+ queue.getName()
						+ "). It cannot be accessed by consumers on another connection, and will be redeclared if the connection is reopened.");
			}
		}

		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				declareExchanges(channel, exchanges.toArray(new Exchange[exchanges.size()]));
				declareQueues(channel, queues.toArray(new Queue[queues.size()]));
				declareBindings(channel, bindings.toArray(new Binding[bindings.size()]));
				return null;
			}
		});
		logger.debug("Declarations finished");

	}

	// private methods for declaring Exchanges, Queues, and Bindings on a Channel

	private void declareExchanges(final Channel channel, final Exchange... exchanges) throws IOException {
		for (final Exchange exchange : exchanges) {
			if (logger.isDebugEnabled()) {
				logger.debug("declaring Exchange '" + exchange.getName() + "'");
			}
			channel.exchangeDeclare(exchange.getName(), exchange.getType(), exchange.isDurable(),
					exchange.isAutoDelete(), exchange.getArguments());
		}
	}

	private void declareQueues(final Channel channel, final Queue... queues) throws IOException {
		for (Queue queue : queues) {
			if (!queue.getName().startsWith("amq.")) {
				if (logger.isDebugEnabled()) {
					logger.debug("declaring Queue '" + queue.getName() + "'");
				}
				channel.queueDeclare(queue.getName(), queue.isDurable(), queue.isExclusive(), queue.isAutoDelete(),
						queue.getArguments());
			} else if (logger.isDebugEnabled()) {
				logger.debug("Queue with name that starts with 'amq.' cannot be declared.");
			}
		}
	}

	private void declareBindings(final Channel channel, final Binding... bindings) throws IOException {
		for (Binding binding : bindings) {
			if (logger.isDebugEnabled()) {
				logger.debug("Binding queue [" + binding.getQueue() + "] to exchange [" + binding.getExchange()
						+ "] with routing key [" + binding.getRoutingKey() + "]");
			}
			channel.queueBind(binding.getQueue(), binding.getExchange(), binding.getRoutingKey(),
					binding.getArguments());
		}
	}

}
