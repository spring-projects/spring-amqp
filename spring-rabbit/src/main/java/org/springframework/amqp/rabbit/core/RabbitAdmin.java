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

package org.springframework.amqp.rabbit.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.util.Assert;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.Channel;

/**
 * RabbitMQ implementation of portable AMQP administrative operations for AMQP &gt;= 0.9.1
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Ed Scriven
 * @author Gary Russell
 * @author Artem Bilan
 */
public class RabbitAdmin implements AmqpAdmin, ApplicationContextAware, ApplicationEventPublisherAware,
		InitializingBean {

	/**
	 * The default exchange name.
	 */
	public static final String DEFAULT_EXCHANGE_NAME = "";

	/**
	 * Property key for the queue name in the {@link Properties} returned by
	 * {@link #getQueueProperties(String)}.
	 */
	public static final Object QUEUE_NAME = "QUEUE_NAME";

	/**
	 * Property key for the message count in the {@link Properties} returned by
	 * {@link #getQueueProperties(String)}.
	 */
	public static final Object QUEUE_MESSAGE_COUNT = "QUEUE_MESSAGE_COUNT";

	/**
	 * Property key for the consumer count in the {@link Properties} returned by
	 * {@link #getQueueProperties(String)}.
	 */
	public static final Object QUEUE_CONSUMER_COUNT = "QUEUE_CONSUMER_COUNT";

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private final RabbitTemplate rabbitTemplate;

	private volatile boolean running = false;

	private volatile boolean autoStartup = true;

	private volatile ApplicationContext applicationContext;

	private volatile boolean ignoreDeclarationExceptions;

	private final Object lifecycleMonitor = new Object();

	private final ConnectionFactory connectionFactory;

	private ApplicationEventPublisher applicationEventPublisher;

	private volatile DeclarationExceptionEvent lastDeclarationExceptionEvent;

	public RabbitAdmin(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	public void setIgnoreDeclarationExceptions(boolean ignoreDeclarationExceptions) {
		this.ignoreDeclarationExceptions = ignoreDeclarationExceptions;
	}

	/**
	 * @return the last {@link DeclarationExceptionEvent} that was detected in this admin.
	 *
	 * @since 1.6
	 */
	public DeclarationExceptionEvent getLastDeclarationExceptionEvent() {
		return this.lastDeclarationExceptionEvent;
	}

	public RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}

	// Exchange operations

	@Override
	public void declareExchange(final Exchange exchange) {
		try {
			this.rabbitTemplate.execute(new ChannelCallback<Object>() {
				@Override
				public Object doInRabbit(Channel channel) throws Exception {
					declareExchanges(channel, exchange);
					return null;
				}
			});
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(exchange, "exchange", e);
		}
	}

	@Override
	@ManagedOperation
	public boolean deleteExchange(final String exchangeName) {
		return this.rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			@Override
			public Boolean doInRabbit(Channel channel) throws Exception {
				if (isDeletingDefaultExchange(exchangeName)) {
					return true;
				}

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

	/**
	 * Declare the given queue.
	 * If the queue doesn't have a value for 'name' property,
	 * the queue name will be generated by Broker and returned from this method.
	 * But the 'name' property of the queue remains as is.
	 * @param queue the queue
	 * @return the queue name if successful, null if not successful and
	 * {@link #setIgnoreDeclarationExceptions(boolean) ignoreDeclarationExceptions} is
	 * true.
	 */
	@Override
	@ManagedOperation
	public String declareQueue(final Queue queue) {
		try {
			return this.rabbitTemplate.execute(new ChannelCallback<String>() {
				@Override
				public String doInRabbit(Channel channel) throws Exception {
					DeclareOk[] declared = declareQueues(channel, queue);
					return declared.length > 0 ? declared[0].getQueue() : null;
				}
			});
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(queue, "queue", e);
			return null;
		}
	}

	/**
	 * Declares a server-named exclusive, autodelete, non-durable queue.
	 *
	 * @return the queue or null if an exception occurred and
	 * {@link #setIgnoreDeclarationExceptions(boolean) ignoreDeclarationExceptions}
	 * is true.
	 */
	@Override
	@ManagedOperation
	public Queue declareQueue() {
		try {
			DeclareOk declareOk = this.rabbitTemplate.execute(new ChannelCallback<DeclareOk>() {
				@Override
				public DeclareOk doInRabbit(Channel channel) throws Exception {
					return channel.queueDeclare();
				}
			});
			Queue queue = new Queue(declareOk.getQueue(), false, true, true);
			return queue;
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(null, "queue", e);
			return null;
		}
	}

	@Override
	@ManagedOperation
	public boolean deleteQueue(final String queueName) {
		return this.rabbitTemplate.execute(new ChannelCallback<Boolean>() {
			@Override
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

	@Override
	@ManagedOperation
	public void deleteQueue(final String queueName, final boolean unused, final boolean empty) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			@Override
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDelete(queueName, unused, empty);
				return null;
			}
		});
	}

	@Override
	@ManagedOperation
	public void purgeQueue(final String queueName, final boolean noWait) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			@Override
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queuePurge(queueName);
				return null;
			}
		});
	}

	// Binding
	@Override
	@ManagedOperation
	public void declareBinding(final Binding binding) {
		try {
			this.rabbitTemplate.execute(new ChannelCallback<Object>() {
				@Override
				public Object doInRabbit(Channel channel) throws Exception {
					declareBindings(channel, binding);
					return null;
				}
			});
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(binding, "binding", e);
		}
	}

	@Override
	@ManagedOperation
	public void removeBinding(final Binding binding) {
		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			@Override
			public Object doInRabbit(Channel channel) throws Exception {
				if (binding.isDestinationQueue()) {
					if (isRemovingImplicitQueueBinding(binding)) {
						return null;
					}

					channel.queueUnbind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
							binding.getArguments());
				} else {
					channel.exchangeUnbind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
							binding.getArguments());
				}
				return null;
			}
		});
	}

	/**
	 * Returns 3 properties {@link #QUEUE_NAME}, {@link #QUEUE_MESSAGE_COUNT},
	 * {@link #QUEUE_CONSUMER_COUNT}, or null if the queue doesn't exist.
	 */
	@Override
	public Properties getQueueProperties(final String queueName) {
		Assert.hasText(queueName, "'queueName' cannot be null or empty");
		return this.rabbitTemplate.execute(new ChannelCallback<Properties>() {
			@Override
			public Properties doInRabbit(Channel channel) throws Exception {
				try {
					DeclareOk declareOk = channel.queueDeclarePassive(queueName);
					Properties props = new Properties();
					props.put(QUEUE_NAME, declareOk.getQueue());
					props.put(QUEUE_MESSAGE_COUNT, declareOk.getMessageCount());
					props.put(QUEUE_CONSUMER_COUNT, declareOk.getConsumerCount());
					return props;
				}
				catch (IllegalArgumentException e) {
					if (RabbitAdmin.this.logger.isDebugEnabled()) {
						RabbitAdmin.this.logger.error("Exception while fetching Queue properties: '" + queueName + "'",
								e);
					}
					try {
						if (channel instanceof ChannelProxy) {
							((ChannelProxy) channel).getTargetChannel().close();
						}
					}
					catch (TimeoutException e1) {
					}
					return null;
				}
				catch (Exception e) {
					if (RabbitAdmin.this.logger.isDebugEnabled()) {
						RabbitAdmin.this.logger.debug("Queue '" + queueName + "' does not exist");
					}
					return null;
				}
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
	@Override
	public void afterPropertiesSet() {

		synchronized (this.lifecycleMonitor) {

			if (this.running || !this.autoStartup) {
				return;
			}

			if (this.connectionFactory instanceof CachingConnectionFactory &&
					((CachingConnectionFactory) this.connectionFactory).getCacheMode() == CacheMode.CONNECTION) {
				this.logger.warn("RabbitAdmin auto declaration is not supported with CacheMode.CONNECTION");
				return;
			}

			this.connectionFactory.addConnectionListener(new ConnectionListener() {

				// Prevent stack overflow...
				private final AtomicBoolean initializing = new AtomicBoolean(false);

				@Override
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
					}
					finally {
						initializing.compareAndSet(true, false);
					}
				}

				@Override
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

		this.logger.debug("Initializing declarations");
		Collection<Exchange> contextExchanges = new LinkedList<Exchange>(this.applicationContext.getBeansOfType(Exchange.class).values());
		Collection<Queue> contextQueues = new LinkedList<Queue>(this.applicationContext.getBeansOfType(Queue.class).values());
		Collection<Binding> contextBindings = new LinkedList<Binding>(this.applicationContext.getBeansOfType(Binding.class).values());

		@SuppressWarnings("rawtypes")
		Collection<Collection> collections = this.applicationContext.getBeansOfType(Collection.class).values();
		for (Collection<?> collection : collections) {
			if (collection.size() > 0 && collection.iterator().next() instanceof Declarable) {
				for (Object declarable : collection) {
					if (declarable instanceof Exchange) {
						contextExchanges.add((Exchange) declarable);
					}
					else if (declarable instanceof Queue) {
						contextQueues.add((Queue) declarable);
					}
					else if (declarable instanceof Binding) {
						contextBindings.add((Binding) declarable);
					}
				}
			}
		}

		final Collection<Exchange> exchanges = filterDeclarables(contextExchanges);
		final Collection<Queue> queues = filterDeclarables(contextQueues);
		final Collection<Binding> bindings = filterDeclarables(contextBindings);

		for (Exchange exchange : exchanges) {
			if (!exchange.isDurable() || exchange.isAutoDelete()) {
				this.logger.info("Auto-declaring a non-durable or auto-delete Exchange ("
						+ exchange.getName()
						+ ") durable:" + exchange.isDurable() + ", auto-delete:" + exchange.isAutoDelete() + ". "
						+ "It will be deleted by the broker if it shuts down, and can be redeclared by closing and "
						+ "reopening the connection.");
			}
		}

		for (Queue queue : queues) {
			if (!queue.isDurable() || queue.isAutoDelete() || queue.isExclusive()) {
				this.logger.info("Auto-declaring a non-durable, auto-delete, or exclusive Queue ("
						+ queue.getName()
						+ ") durable:" + queue.isDurable() + ", auto-delete:" + queue.isAutoDelete() + ", exclusive:"
						+ queue.isExclusive() + ". "
						+ "It will be redeclared if the broker stops and is restarted while the connection factory is "
						+ "alive, but all messages will be lost.");
			}
		}

		this.rabbitTemplate.execute(new ChannelCallback<Object>() {
			@Override
			public Object doInRabbit(Channel channel) throws Exception {
				declareExchanges(channel, exchanges.toArray(new Exchange[exchanges.size()]));
				declareQueues(channel, queues.toArray(new Queue[queues.size()]));
				declareBindings(channel, bindings.toArray(new Binding[bindings.size()]));
				return null;
			}
		});
		this.logger.debug("Declarations finished");

	}

	/**
	 * Remove any instances that should not be declared by this admin.
	 * @param declarables the collection of {@link Declarable}s.
	 * @return a new collection containing {@link Declarable}s that should be declared by this
	 * admin.
	 */
	private <T extends Declarable> Collection<T> filterDeclarables(Collection<T> declarables) {
		Collection<T> filtered = new ArrayList<T>();
		for (T declarable : declarables) {
			Collection<?> adminsWithWhichToDeclare = declarable.getDeclaringAdmins();
			if (declarable.shouldDeclare() &&
				(adminsWithWhichToDeclare.isEmpty() || adminsWithWhichToDeclare.contains(this))) {
				filtered.add(declarable);
			}
		}
		return filtered;
	}

	// private methods for declaring Exchanges, Queues, and Bindings on a Channel

	private void declareExchanges(final Channel channel, final Exchange... exchanges) throws IOException {
		for (final Exchange exchange : exchanges) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("declaring Exchange '" + exchange.getName() + "'");
			}

			if (!isDeclaringDefaultExchange(exchange)) {
				try {
					if (exchange.isDelayed()) {
						Map<String, Object> arguments = exchange.getArguments();
						if (arguments == null) {
							arguments = new HashMap<String, Object>();
						}
						else {
							arguments = new HashMap<String, Object>(arguments);
						}
						arguments.put("x-delayed-type", exchange.getType());
						channel.exchangeDeclare(exchange.getName(), ExchangeTypes.DELAYED, exchange.isDurable(),
								exchange.isAutoDelete(), arguments);
					}
					else {
						channel.exchangeDeclare(exchange.getName(), exchange.getType(), exchange.isDurable(),
							exchange.isAutoDelete(), exchange.getArguments());
					}
				}
				catch (IOException e) {
					logOrRethrowDeclarationException(exchange, "exchange", e);
				}
			}
		}
	}

	private DeclareOk[] declareQueues(final Channel channel, final Queue... queues) throws IOException {
		List<DeclareOk> declareOks = new ArrayList<DeclareOk>(queues.length);
		for (int i = 0; i < queues.length; i++) {
			Queue queue = queues[i];
			if (!queue.getName().startsWith("amq.")) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("declaring Queue '" + queue.getName() + "'");
				}
				try {
					try {
						DeclareOk declareOk = channel.queueDeclare(queue.getName(), queue.isDurable(), queue.isExclusive(), queue.isAutoDelete(),
								queue.getArguments());
						declareOks.add(declareOk);
					}
					catch (IllegalArgumentException e) {
						if (this.logger.isDebugEnabled()) {
							this.logger.error("Exception while declaring queue: '" + queue.getName() + "'");
						}
						try {
							if (channel instanceof ChannelProxy) {
								((ChannelProxy) channel).getTargetChannel().close();
							}
						}
						catch (TimeoutException e1) {
						}
						throw new IOException(e);
					}
				}
				catch (IOException e) {
					logOrRethrowDeclarationException(queue, "queue", e);
				}
			} else if (this.logger.isDebugEnabled()) {
				this.logger.debug("Queue with name that starts with 'amq.' cannot be declared.");
			}
		}
		return declareOks.toArray(new DeclareOk[declareOks.size()]);
	}

	private void declareBindings(final Channel channel, final Binding... bindings) throws IOException {
		for (Binding binding : bindings) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Binding destination [" + binding.getDestination() + " (" + binding.getDestinationType()
						+ ")] to exchange [" + binding.getExchange() + "] with routing key [" + binding.getRoutingKey()
						+ "]");
			}

			try {
				if (binding.isDestinationQueue()) {
					if (!isDeclaringImplicitQueueBinding(binding)) {
						channel.queueBind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
								binding.getArguments());
					}
				} else {
					channel.exchangeBind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
							binding.getArguments());
				}
			}
			catch (IOException e) {
				logOrRethrowDeclarationException(binding, "binding", e);
			}
		}
	}

	private <T extends Throwable> void logOrRethrowDeclarationException(Declarable element, String elementType, T t)
			throws T {
		DeclarationExceptionEvent event = new DeclarationExceptionEvent(this, element, t);
		this.lastDeclarationExceptionEvent = event;
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
		if (this.ignoreDeclarationExceptions) {
			if (this.logger.isWarnEnabled()) {
				this.logger.warn("Failed to declare " + elementType
						+ (element == null ? "broker-generated" : ": " + element)
						+ ", continuing...", t);
			}
		}
		else {
			throw t;
		}
	}

	private boolean isDeclaringDefaultExchange(Exchange exchange) {
		if (isDefaultExchange(exchange.getName())) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Default exchange is pre-declared by server.");
			}
			return true;
		}
		return false;
	}

	private boolean isDeletingDefaultExchange(String exchangeName) {
		if (isDefaultExchange(exchangeName)) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Default exchange cannot be deleted.");
			}
			return true;
		}
		return false;
	}

	private boolean isDefaultExchange(String exchangeName) {
		return DEFAULT_EXCHANGE_NAME.equals(exchangeName);
	}

	private boolean isDeclaringImplicitQueueBinding(Binding binding) {
		if (isImplicitQueueBinding(binding)) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("The default exchange is implicitly bound to every queue, with a routing key equal to the queue name.");
			}
			return true;
		}
		return false;
	}

	private boolean isRemovingImplicitQueueBinding(Binding binding) {
		if (isImplicitQueueBinding(binding)) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Cannot remove implicit default exchange binding to queue.");
			}
			return true;
		}
		return false;
	}

	private boolean isImplicitQueueBinding(Binding binding) {
		return isDefaultExchange(binding.getExchange()) && binding.getDestination().equals(binding.getRoutingKey());
	}

}
