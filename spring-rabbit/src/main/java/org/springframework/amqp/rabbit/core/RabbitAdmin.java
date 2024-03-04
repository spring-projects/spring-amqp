/*
 * Copyright 2002-2023 the original author or authors.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpException;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DeclarableCustomizer;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.CacheMode;
import org.springframework.amqp.rabbit.connection.ChannelProxy;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.lang.Nullable;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.AMQP.Queue.DeclareOk;
import com.rabbitmq.client.AMQP.Queue.PurgeOk;
import com.rabbitmq.client.Channel;

/**
 * RabbitMQ implementation of portable AMQP administrative operations for AMQP &gt;= 0.9.1.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author Ed Scriven
 * @author Gary Russell
 * @author Artem Bilan
 * @author Christian Tzolov
 */
@ManagedResource(description = "Admin Tasks")
public class RabbitAdmin implements AmqpAdmin, ApplicationContextAware, ApplicationEventPublisherAware,
		BeanNameAware, InitializingBean {

	private static final String UNUSED = "unused";

	private static final int DECLARE_MAX_ATTEMPTS = 5;

	private static final int DECLARE_INITIAL_RETRY_INTERVAL = 1000;

	private static final int DECLARE_MAX_RETRY_INTERVAL = 5000;

	private static final double DECLARE_RETRY_MULTIPLIER = 2.0;

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

	private static final String DELAYED_MESSAGE_EXCHANGE = "x-delayed-message";

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private final Lock lock = new ReentrantLock();

	private final RabbitTemplate rabbitTemplate;

	private final Lock lifecycleLock = new ReentrantLock();

	private final ConnectionFactory connectionFactory;

	private final Set<Declarable> manualDeclarables = Collections.synchronizedSet(new LinkedHashSet<>());

	private final Lock manualDeclarablesLock = new ReentrantLock();

	private String beanName;

	private RetryTemplate retryTemplate;

	private boolean retryDisabled;

	private boolean autoStartup = true;

	private ApplicationContext applicationContext;

	private boolean ignoreDeclarationExceptions;

	private ApplicationEventPublisher applicationEventPublisher;

	private TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean explicitDeclarationsOnly;

	private boolean redeclareManualDeclarations;

	private volatile boolean running = false;

	private volatile DeclarationExceptionEvent lastDeclarationExceptionEvent;

	/**
	 * Construct an instance using the provided {@link ConnectionFactory}.
	 * @param connectionFactory the connection factory - must not be null.
	 */
	public RabbitAdmin(ConnectionFactory connectionFactory) {
		Assert.notNull(connectionFactory, "ConnectionFactory must not be null");
		this.connectionFactory = connectionFactory;
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
	}

	/**
	 * Construct an instance using the provided {@link RabbitTemplate}. Use this
	 * constructor when, for example, you want the admin operations to be performed within
	 * the scope of the provided template's {@code invoke()} method.
	 * @param rabbitTemplate the template - must not be null and must have a connection
	 * factory.
	 * @since 2.0
	 */
	public RabbitAdmin(RabbitTemplate rabbitTemplate) {
		Assert.notNull(rabbitTemplate, "RabbitTemplate must not be null");
		Assert.notNull(rabbitTemplate.getConnectionFactory(), "RabbitTemplate's ConnectionFactory must not be null");
		this.connectionFactory = rabbitTemplate.getConnectionFactory();
		this.rabbitTemplate = rabbitTemplate;
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

	/**
	 * Set a task executor to use for async operations. Currently only used
	 * with {@link #purgeQueue(String, boolean)}.
	 * @param taskExecutor the executor to use.
	 * @since 2.1
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		Assert.notNull(taskExecutor, "'taskExecutor' cannot be null");
		this.taskExecutor = taskExecutor;
	}

	public RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}

	// Exchange operations

	@Override
	public void declareExchange(final Exchange exchange) {
		try {
			this.rabbitTemplate.execute(channel -> {
				declareExchanges(channel, exchange);
				if (this.redeclareManualDeclarations) {
					this.manualDeclarables.add(exchange);
				}
				return null;
			});
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(exchange, "exchange", e);
		}
	}

	@Override
	@ManagedOperation(description = "Delete an exchange from the broker")
	public boolean deleteExchange(final String exchangeName) {
		return this.rabbitTemplate.execute(channel -> { // NOSONAR never returns null
			if (isDeletingDefaultExchange(exchangeName)) {
				return true;
			}

			try {
				channel.exchangeDelete(exchangeName);
				removeExchangeBindings(exchangeName);
			}
			catch (@SuppressWarnings(UNUSED) IOException e) {
				return false;
			}
			return true;
		});
	}

	private void removeExchangeBindings(final String exchangeName) {
		this.manualDeclarablesLock.lock();
		try {
			this.manualDeclarables.stream()
					.filter(dec -> dec instanceof Exchange ex && ex.getName().equals(exchangeName))
					.collect(Collectors.toSet())
					.forEach(this.manualDeclarables::remove);
			this.manualDeclarables.removeIf(next ->
					next instanceof Binding binding
							&& ((!binding.isDestinationQueue() && binding.getDestination().equals(exchangeName))
							|| binding.getExchange().equals(exchangeName)));
		}
		finally {
			this.manualDeclarablesLock.unlock();
		}
	}


	// Queue operations

	/**
	 * Declare the given queue.
	 * If the queue doesn't have a value for 'name' property,
	 * the queue name will be generated by Broker and returned from this method.
	 * The declaredName property of the queue will be updated to reflect this value.
	 * @param queue the queue
	 * @return the queue name if successful, null if not successful and
	 * {@link #setIgnoreDeclarationExceptions(boolean) ignoreDeclarationExceptions} is
	 * true.
	 */
	@Override
	@ManagedOperation(description = "Declare a queue on the broker (this operation is not available remotely)")
	@Nullable
	public String declareQueue(final Queue queue) {
		try {
			return this.rabbitTemplate.execute(channel -> {
				DeclareOk[] declared = declareQueues(channel, queue);
				String result = declared.length > 0 ? declared[0].getQueue() : null;
				if (this.redeclareManualDeclarations) {
					this.manualDeclarables.add(queue);
				}
				return result;
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
	@ManagedOperation(description =
			"Declare a queue with a broker-generated name (this operation is not available remotely)")
	@Nullable
	public Queue declareQueue() {
		try {
			DeclareOk declareOk = this.rabbitTemplate.execute(Channel::queueDeclare);
			return new Queue(declareOk.getQueue(), false, true, true); // NOSONAR never null
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(null, "queue", e);
			return null;
		}
	}

	@Override
	@ManagedOperation(description = "Delete a queue from the broker")
	public boolean deleteQueue(final String queueName) {
		return this.rabbitTemplate.execute(channel -> { // NOSONAR never returns null
			try {
				channel.queueDelete(queueName);
				removeQueueBindings(queueName);
			}
			catch (@SuppressWarnings(UNUSED) IOException e) {
				return false;
			}
			return true;
		});
	}

	@Override
	@ManagedOperation(description =
			"Delete a queue from the broker if unused and empty (when corresponding arguments are true")
	public void deleteQueue(final String queueName, final boolean unused, final boolean empty) {
		this.rabbitTemplate.execute(channel -> {
			channel.queueDelete(queueName, unused, empty);
			removeQueueBindings(queueName);
			return null;
		});
	}

	private void removeQueueBindings(final String queueName) {
		this.manualDeclarablesLock.lock();
		try {
			this.manualDeclarables.stream()
					.filter(dec -> dec instanceof Queue queue && queue.getName().equals(queueName))
					.collect(Collectors.toSet())
					.forEach(this.manualDeclarables::remove);
			this.manualDeclarables.removeIf(next ->
					next instanceof Binding binding
							&& (binding.isDestinationQueue()
							&& binding.getDestination().equals(queueName)));
		}
		finally {
			this.manualDeclarablesLock.unlock();
		}
	}

	@Override
	@ManagedOperation(description = "Purge a queue and optionally don't wait for the purge to occur")
	public void purgeQueue(final String queueName, final boolean noWait) {
		if (noWait) {
			this.taskExecutor.execute(() -> purgeQueue(queueName));
		}
		else {
			purgeQueue(queueName);
		}
	}

	@Override
	@ManagedOperation(description = "Purge a queue and return the number of messages purged")
	public int purgeQueue(final String queueName) {
		return this.rabbitTemplate.execute(channel -> { // NOSONAR never returns null
			PurgeOk queuePurged = channel.queuePurge(queueName);
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Purged queue: " + queueName + ", " + queuePurged);
			}
			return queuePurged.getMessageCount();
		});
	}

	// Binding
	@Override
	@ManagedOperation(description = "Declare a binding on the broker (this operation is not available remotely)")
	public void declareBinding(final Binding binding) {
		try {
			this.rabbitTemplate.execute(channel -> {
				declareBindings(channel, binding);
				if (this.redeclareManualDeclarations) {
					this.manualDeclarables.add(binding);
				}
				return null;
			});
		}
		catch (AmqpException e) {
			logOrRethrowDeclarationException(binding, "binding", e);
		}
	}

	@Override
	@ManagedOperation(description = "Remove a binding from the broker (this operation is not available remotely)")
	public void removeBinding(final Binding binding) {
		this.rabbitTemplate.execute(channel -> {
			if (binding.isDestinationQueue()) {
				if (isRemovingImplicitQueueBinding(binding)) {
					return null;
				}

				channel.queueUnbind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
						binding.getArguments());
			}
			else {
				channel.exchangeUnbind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
						binding.getArguments());
			}
			this.manualDeclarables.remove(binding.toString());
			return null;
		});
	}

	/**
	 * Returns 3 properties {@link #QUEUE_NAME}, {@link #QUEUE_MESSAGE_COUNT},
	 * {@link #QUEUE_CONSUMER_COUNT}, or null if the queue doesn't exist.
	 */
	@Override
	@ManagedOperation(description = "Get queue name, message count and consumer count")
	public Properties getQueueProperties(final String queueName) {
		QueueInformation queueInfo = getQueueInfo(queueName);
		if (queueInfo != null) {
			Properties props = new Properties();
			props.put(QUEUE_NAME, queueInfo.getName());
			props.put(QUEUE_MESSAGE_COUNT, queueInfo.getMessageCount());
			props.put(QUEUE_CONSUMER_COUNT, queueInfo.getConsumerCount());
			return props;
		}
		else {
			return null;
		}
	}

	@Override
	public QueueInformation getQueueInfo(String queueName) {
		Assert.hasText(queueName, "'queueName' cannot be null or empty");
		return this.rabbitTemplate.execute(channel -> {
			try {
				DeclareOk declareOk = channel.queueDeclarePassive(queueName);
				return new QueueInformation(declareOk.getQueue(), declareOk.getMessageCount(),
						declareOk.getConsumerCount());
			}
			catch (IllegalArgumentException e) {
				if (RabbitAdmin.this.logger.isDebugEnabled()) {
					RabbitAdmin.this.logger.error("Exception while fetching Queue properties: '" + queueName + "'",
							e);
				}
				try {
					if (channel instanceof ChannelProxy proxy) {
						proxy.getTargetChannel().close();
					}
				}
				catch (@SuppressWarnings(UNUSED) TimeoutException e1) {
				}
				return null;
			}
			catch (@SuppressWarnings(UNUSED) Exception e) {
				if (RabbitAdmin.this.logger.isDebugEnabled()) {
					RabbitAdmin.this.logger.debug("Queue '" + queueName + "' does not exist");
				}
				return null;
			}
		});
	}

	/**
	 * Set to true to only declare {@link Declarable} beans that are explicitly configured
	 * to be declared by this admin.
	 * @param explicitDeclarationsOnly true to ignore beans with no admin declaration
	 * configuration.
	 * @since 2.1.9
	 */
	public void setExplicitDeclarationsOnly(boolean explicitDeclarationsOnly) {
		this.explicitDeclarationsOnly = explicitDeclarationsOnly;
	}

	/**
	 * Normally, when a connection is recovered, the admin only recovers auto-delete queues,
	 * etc, that are declared as beans in the application context. When this is true, it
	 * will also redeclare any manually declared {@link Declarable}s via admin methods.
	 * @return true to redeclare.
	 * @since 2.4
	 */
	public boolean isRedeclareManualDeclarations() {
		return this.redeclareManualDeclarations;
	}

	/**
	 * Normally, when a connection is recovered, the admin only recovers auto-delete
	 * queues, etc., that are declared as beans in the application context. When this is
	 * true, it will also redeclare any manually declared {@link Declarable}s via admin
	 * methods. When a queue or exchange is deleted, it will no longer be recovered, nor
	 * will any corresponding bindings.
	 * @param redeclareManualDeclarations true to redeclare.
	 * @since 2.4
	 * @see #declareQueue(Queue)
	 * @see #declareExchange(Exchange)
	 * @see #declareBinding(Binding)
	 * @see #deleteQueue(String)
	 * @see #deleteExchange(String)
	 * @see #removeBinding(Binding)
	 * @see #resetAllManualDeclarations()
	 */
	public void setRedeclareManualDeclarations(boolean redeclareManualDeclarations) {
		this.redeclareManualDeclarations = redeclareManualDeclarations;
	}

	/**
	 * Set a retry template for auto declarations. There is a race condition with
	 * auto-delete, exclusive queues in that the queue might still exist for a short time,
	 * preventing the redeclaration. The default retry configuration will try 5 times with
	 * an exponential backOff starting at 1 second a multiplier of 2.0 and a max interval
	 * of 5 seconds. To disable retry, set the argument to {@code null}. Note that this
	 * retry is at the macro level - all declarations will be retried within the scope of
	 * this template. If you supplied a {@link RabbitTemplate} that is configured with a
	 * {@link RetryTemplate}, its template will retry each individual declaration.
	 * @param retryTemplate the retry template.
	 * @since 1.7.8
	 */
	public void setRetryTemplate(@Nullable RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
		if (retryTemplate == null) {
			this.retryDisabled = true;
		}
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public String getBeanName() {
		return this.beanName;
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
		this.lifecycleLock.lock();
		try {

			if (this.running || !this.autoStartup) {
				return;
			}

			if (this.retryTemplate == null && !this.retryDisabled) {
				this.retryTemplate = new RetryTemplate();
				this.retryTemplate.setRetryPolicy(new SimpleRetryPolicy(DECLARE_MAX_ATTEMPTS));
				ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
				backOffPolicy.setInitialInterval(DECLARE_INITIAL_RETRY_INTERVAL);
				backOffPolicy.setMultiplier(DECLARE_RETRY_MULTIPLIER);
				backOffPolicy.setMaxInterval(DECLARE_MAX_RETRY_INTERVAL);
				this.retryTemplate.setBackOffPolicy(backOffPolicy);
			}
			if (this.connectionFactory instanceof CachingConnectionFactory ccf &&
					ccf.getCacheMode() == CacheMode.CONNECTION) {
				this.logger.warn("RabbitAdmin auto declaration is not supported with CacheMode.CONNECTION");
				return;
			}

			// Prevent stack overflow...
			final AtomicBoolean initializing = new AtomicBoolean(false);

			this.connectionFactory.addConnectionListener(connection -> {

				if (!initializing.compareAndSet(false, true)) {
					// If we are already initializing, we don't need to do it again...
					return;
				}
				try {
					/*
					 * ...but it is possible for this to happen twice in the same ConnectionFactory (if more than
					 * one concurrent Connection is allowed). It's idempotent, so no big deal (a bit of network
					 * chatter). In fact, it might even be a good thing: exclusive queues only make sense if they are
					 * declared for every connection. If anyone has a problem with it: use auto-startup="false".
					 */
					if (this.retryTemplate != null) {
						this.retryTemplate.execute(c -> {
							initialize();
							return null;
						});
					}
					else {
						initialize();
					}
				}
				finally {
					initializing.compareAndSet(true, false);
				}

			});

			this.running = true;
		}
		finally {
			this.lifecycleLock.unlock();
		}
	}

	/**
	 * Declares all the exchanges, queues and bindings in the enclosing application context, if any. It should be safe
	 * (but unnecessary) to call this method more than once.
	 */
	@Override // NOSONAR complexity
	public void initialize() {

		redeclareManualDeclarables();

		if (this.applicationContext == null) {
			this.logger.debug("no ApplicationContext has been set, cannot auto-declare Exchanges, Queues, and Bindings");
			return;
		}

		this.logger.debug("Initializing declarations");
		Collection<Exchange> contextExchanges = new LinkedList<>(
				this.applicationContext.getBeansOfType(Exchange.class).values());
		Collection<Queue> contextQueues = new LinkedList<>(
				this.applicationContext.getBeansOfType(Queue.class).values());
		Collection<Binding> contextBindings = new LinkedList<>(
				this.applicationContext.getBeansOfType(Binding.class).values());
		Collection<DeclarableCustomizer> customizers =
				this.applicationContext.getBeansOfType(DeclarableCustomizer.class).values();

		processDeclarables(contextExchanges, contextQueues, contextBindings);

		final Collection<Exchange> exchanges = filterDeclarables(contextExchanges, customizers);
		final Collection<Queue> queues = filterDeclarables(contextQueues, customizers);
		final Collection<Binding> bindings = filterDeclarables(contextBindings, customizers);

		for (Exchange exchange : exchanges) {
			if ((!exchange.isDurable() || exchange.isAutoDelete()) && this.logger.isInfoEnabled()) {
				this.logger.info("Auto-declaring a non-durable or auto-delete Exchange ("
						+ exchange.getName()
						+ ") durable:" + exchange.isDurable() + ", auto-delete:" + exchange.isAutoDelete() + ". "
						+ "It will be deleted by the broker if it shuts down, and can be redeclared by closing and "
						+ "reopening the connection.");
			}
		}

		for (Queue queue : queues) {
			if ((!queue.isDurable() || queue.isAutoDelete() || queue.isExclusive()) && this.logger.isInfoEnabled()) {
				this.logger.info("Auto-declaring a non-durable, auto-delete, or exclusive Queue ("
						+ queue.getName()
						+ ") durable:" + queue.isDurable() + ", auto-delete:" + queue.isAutoDelete() + ", exclusive:"
						+ queue.isExclusive() + ". "
						+ "It will be redeclared if the broker stops and is restarted while the connection factory is "
						+ "alive, but all messages will be lost.");
			}
		}

		if (exchanges.isEmpty() && queues.isEmpty() && bindings.isEmpty() && this.manualDeclarables.isEmpty()) {
			this.logger.debug("Nothing to declare");
			return;
		}
		this.rabbitTemplate.execute(channel -> {
			declareExchanges(channel, exchanges.toArray(new Exchange[0]));
			declareQueues(channel, queues.toArray(new Queue[0]));
			declareBindings(channel, bindings.toArray(new Binding[0]));
			return null;
		});
		this.logger.debug("Declarations finished");

	}

	/**
	 * Process manual declarables.
	 */
	private void redeclareManualDeclarables() {
		if (!this.manualDeclarables.isEmpty()) {
			this.manualDeclarablesLock.lock();
			try {
				this.logger.debug("Redeclaring manually declared Declarables");
				for (Declarable dec : this.manualDeclarables) {
					if (dec instanceof Queue queue) {
						declareQueue(queue);
					}
					else if (dec instanceof Exchange exch) {
						declareExchange(exch);
					}
					else {
						declareBinding((Binding) dec);
					}
				}
			}
			finally {
				this.manualDeclarablesLock.unlock();
			}
		}

	}

	/**
	 * Invoke this method to prevent the admin from recovering any declarations made
	 * by calls to {@code declare*()} methods.
	 * @since 2.4
	 * @see #setRedeclareManualDeclarations(boolean)
	 */
	public void resetAllManualDeclarations() {
		this.manualDeclarables.clear();
	}

	@Override
	@Deprecated
	public Map<String, Declarable> getManualDeclarables() {
		Map<String, Declarable> declarables = new HashMap<>();
		this.manualDeclarables.forEach(declarable -> {
			if (declarable instanceof Exchange exch) {
				declarables.put(exch.getName(), declarable);
			}
			else if (declarable instanceof Queue queue) {
				declarables.put(queue.getName(), declarable);
			}
			else if (declarable instanceof Binding) {
				declarables.put(declarable.toString(), declarable);
			}
		});
		return declarables;
	}

	@Override
	public Set<Declarable> getManualDeclarableSet() {
		return Collections.unmodifiableSet(this.manualDeclarables);
	}

	private void processDeclarables(Collection<Exchange> contextExchanges, Collection<Queue> contextQueues,
			Collection<Binding> contextBindings) {

		Collection<Declarables> declarables = this.applicationContext.getBeansOfType(Declarables.class, false, true)
				.values();
		declarables.forEach(d -> {
			d.getDeclarables().forEach(declarable -> {
				if (declarable instanceof Exchange exch) {
					contextExchanges.add(exch);
				}
				else if (declarable instanceof Queue queue) {
					contextQueues.add(queue);
				}
				else if (declarable instanceof Binding binding) {
					contextBindings.add(binding);
				}
			});
		});
	}

	/**
	 * Remove any instances that should not be declared by this admin.
	 * @param declarables the collection of {@link Declarable}s.
	 * @param customizers a collection if {@link DeclarableCustomizer} beans.
	 * @param <T> the declarable type.
	 * @return a new collection containing {@link Declarable}s that should be declared by this
	 * admin.
	 */
	@SuppressWarnings("unchecked")
	private <T extends Declarable> Collection<T> filterDeclarables(Collection<T> declarables,
			Collection<DeclarableCustomizer> customizers) {

		return declarables.stream()
				.filter(dec -> dec.shouldDeclare() && declarableByMe(dec))
				.map(dec -> {
					if (customizers.isEmpty()) {
						return dec;
					}
					AtomicReference<T> ref = new AtomicReference<>(dec);
					customizers.forEach(cust -> ref.set((T) cust.apply(ref.get())));
					return ref.get();
				})
				.toList();
	}

	private <T extends Declarable> boolean declarableByMe(T dec) {
		return (dec.getDeclaringAdmins().isEmpty() && !this.explicitDeclarationsOnly) // NOSONAR boolean complexity
				|| dec.getDeclaringAdmins().contains(this)
				|| (this.beanName != null && dec.getDeclaringAdmins().contains(this.beanName));
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
							arguments = new HashMap<>();
						}
						else {
							arguments = new HashMap<>(arguments);
						}
						arguments.put("x-delayed-type", exchange.getType());
						channel.exchangeDeclare(exchange.getName(), DELAYED_MESSAGE_EXCHANGE, exchange.isDurable(),
								exchange.isAutoDelete(), exchange.isInternal(), arguments);
					}
					else {
						channel.exchangeDeclare(exchange.getName(), exchange.getType(), exchange.isDurable(),
								exchange.isAutoDelete(), exchange.isInternal(), exchange.getArguments());
					}
				}
				catch (IOException e) {
					logOrRethrowDeclarationException(exchange, "exchange", e);
				}
			}
		}
	}

	private DeclareOk[] declareQueues(final Channel channel, final Queue... queues) throws IOException {
		List<DeclareOk> declareOks = new ArrayList<>(queues.length);
		for (Queue queue : queues) {
			if (!queue.getName().startsWith("amq.")) {
				if (this.logger.isDebugEnabled()) {
					this.logger.debug("declaring Queue '" + queue.getName() + "'");
				}
				try {
					try {
						DeclareOk declareOk = channel.queueDeclare(queue.getName(), queue.isDurable(),
								queue.isExclusive(), queue.isAutoDelete(), queue.getArguments());
						if (StringUtils.hasText(declareOk.getQueue())) {
							queue.setActualName(declareOk.getQueue());
						}
						declareOks.add(declareOk);
					}
					catch (IllegalArgumentException e) {
						closeChannelAfterIllegalArg(channel, queue);
						throw new IOException(e);
					}
				}
				catch (IOException e) { // NOSONAR exceptions for flow control
					logOrRethrowDeclarationException(queue, "queue", e);
				}
			}
			else if (this.logger.isDebugEnabled()) {
				this.logger.debug(queue.getName() + ": Queue with name that starts with 'amq.' cannot be declared.");
			}
		}
		return declareOks.toArray(new DeclareOk[0]);
	}

	private void closeChannelAfterIllegalArg(final Channel channel, Queue queue) {
		if (this.logger.isDebugEnabled()) {
			this.logger.error("Exception while declaring queue: '" + queue.getName() + "'");
		}
		try {
			if (channel instanceof ChannelProxy proxy) {
				proxy.getTargetChannel().close();
			}
		}
		catch (IOException | TimeoutException e1) {
			this.logger.error("Failed to close channel after illegal argument", e1);
		}
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
				}
				else {
					channel.exchangeBind(binding.getDestination(), binding.getExchange(), binding.getRoutingKey(),
							binding.getArguments());
				}
			}
			catch (IOException e) {
				logOrRethrowDeclarationException(binding, "binding", e);
			}
		}
	}

	private <T extends Throwable> void logOrRethrowDeclarationException(@Nullable Declarable element,
			String elementType, T t) throws T {

		publishDeclarationExceptionEvent(element, t);
		if (this.ignoreDeclarationExceptions || (element != null && element.isIgnoreDeclarationExceptions())) {
			if (this.logger.isDebugEnabled()) {
				this.logger.debug("Failed to declare " + elementType
						+ ": " + (element == null ? "broker-generated" : element)
						+ ", continuing...", t);
			}
			else if (this.logger.isWarnEnabled()) {
				Throwable cause = t;
				if (t instanceof IOException && t.getCause() != null) {
					cause = t.getCause();
				}
				this.logger.warn("Failed to declare " + elementType
						+ ": " + (element == null ? "broker-generated" : element)
						+ ", continuing... " + cause);
			}
		}
		else {
			throw t;
		}
	}

	private <T extends Throwable> void publishDeclarationExceptionEvent(@Nullable Declarable element, T t) {
		DeclarationExceptionEvent event = new DeclarationExceptionEvent(this, element, t);
		this.lastDeclarationExceptionEvent = event;
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
	}

	private boolean isDeclaringDefaultExchange(Exchange exchange) {
		if (isDefaultExchange(exchange.getName())) {
			this.logger.debug("Default exchange is pre-declared by server.");
			return true;
		}
		return false;
	}

	private boolean isDeletingDefaultExchange(String exchangeName) {
		if (isDefaultExchange(exchangeName)) {
			this.logger.debug("Default exchange cannot be deleted.");
			return true;
		}
		return false;
	}

	private boolean isDefaultExchange(String exchangeName) {
		return DEFAULT_EXCHANGE_NAME.equals(exchangeName);
	}

	private boolean isDeclaringImplicitQueueBinding(Binding binding) {
		if (isImplicitQueueBinding(binding)) {
			this.logger.debug("The default exchange is implicitly bound to every queue," +
					" with a routing key equal to the queue name.");
			return true;
		}
		return false;
	}

	private boolean isRemovingImplicitQueueBinding(Binding binding) {
		if (isImplicitQueueBinding(binding)) {
			this.logger.debug("Cannot remove implicit default exchange binding to queue.");
			return true;
		}
		return false;
	}

	private boolean isImplicitQueueBinding(Binding binding) {
		return isDefaultExchange(binding.getExchange()) && binding.getDestination().equals(binding.getRoutingKey());
	}

}
