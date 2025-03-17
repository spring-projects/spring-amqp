/*
 * Copyright 2025 the original author or authors.
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

package org.springframework.amqp.rabbitmq.client;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import com.rabbitmq.client.amqp.AmqpException;
import com.rabbitmq.client.amqp.Management;
import org.jspecify.annotations.Nullable;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.DeclarableCustomizer;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.DeclarationExceptionEvent;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;
import org.springframework.util.Assert;

/**
 * The {@link AmqpAdmin} implementation for RabbitMQ AMQP 1.0 client.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 */
@ManagedResource(description = "Admin Tasks")
public class RabbitAmqpAdmin
		implements AmqpAdmin, ApplicationContextAware, ApplicationEventPublisherAware, BeanNameAware, SmartLifecycle {

	private static final LogAccessor LOG = new LogAccessor(RabbitAmqpAdmin.class);

	public static final String QUEUE_TYPE = "QUEUE_TYPE";

	private final AmqpConnectionFactory connectionFactory;

	private TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean ignoreDeclarationExceptions;

	private @Nullable ApplicationContext applicationContext;

	private @Nullable ApplicationEventPublisher applicationEventPublisher;

	@SuppressWarnings("NullAway.Init")
	private String beanName;

	private boolean explicitDeclarationsOnly;

	private boolean autoStartup = true;

	private volatile @Nullable DeclarationExceptionEvent lastDeclarationExceptionEvent;

	private volatile boolean running = false;

	public RabbitAmqpAdmin(AmqpConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	public void setIgnoreDeclarationExceptions(boolean ignoreDeclarationExceptions) {
		this.ignoreDeclarationExceptions = ignoreDeclarationExceptions;
	}

	/**
	 * Set a task executor to use for async operations. Currently only used
	 * with {@link #purgeQueue(String, boolean)}.
	 * @param taskExecutor the executor to use.
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		Assert.notNull(taskExecutor, "'taskExecutor' cannot be null");
		this.taskExecutor = taskExecutor;
	}

	/**
	 * Set to true to only declare {@link Declarable} beans that are explicitly configured
	 * to be declared by this admin.
	 * @param explicitDeclarationsOnly true to ignore beans with no admin declaration
	 * configuration.
	 */
	public void setExplicitDeclarationsOnly(boolean explicitDeclarationsOnly) {
		this.explicitDeclarationsOnly = explicitDeclarationsOnly;
	}

	/**
	 * @return the last {@link DeclarationExceptionEvent} that was detected in this admin.
	 */
	public @Nullable DeclarationExceptionEvent getLastDeclarationExceptionEvent() {
		return this.lastDeclarationExceptionEvent;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public int getPhase() {
		return Integer.MIN_VALUE;
	}

	@Override
	public void start() {
		if (!this.running) {
			initialize();
			this.running = true;
		}
	}

	@Override
	public void stop() {
		this.running = false;
	}

	@Override
	public boolean isRunning() {
		return this.running;
	}

	/**
	 * Declares all the exchanges, queues and bindings in the enclosing application context, if any. It should be safe
	 * (but unnecessary) to call this method more than once.
	 */
	@Override
	public void initialize() {
		declareDeclarableBeans();
	}

	/**
	 * Process bean declarables.
	 */
	private void declareDeclarableBeans() {
		if (this.applicationContext == null) {
			LOG.debug("no ApplicationContext has been set, cannot auto-declare Exchanges, Queues, and Bindings");
			return;
		}

		LOG.debug("Initializing declarations");
		Collection<Exchange> contextExchanges = new LinkedList<>(
				this.applicationContext.getBeansOfType(Exchange.class).values());
		Collection<Queue> contextQueues = new LinkedList<>(
				this.applicationContext.getBeansOfType(Queue.class).values());
		Collection<Binding> contextBindings = new LinkedList<>(
				this.applicationContext.getBeansOfType(Binding.class).values());
		Collection<DeclarableCustomizer> customizers =
				this.applicationContext.getBeansOfType(DeclarableCustomizer.class).values();

		processDeclarables(contextExchanges, contextQueues, contextBindings,
				this.applicationContext.getBeansOfType(Declarables.class, false, true).values());

		final Collection<Exchange> exchanges = filterDeclarables(contextExchanges, customizers);
		final Collection<Queue> queues = filterDeclarables(contextQueues, customizers);
		final Collection<Binding> bindings = filterDeclarables(contextBindings, customizers);

		for (Exchange exchange : exchanges) {
			if ((!exchange.isDurable() || exchange.isAutoDelete())) {
				LOG.info(() -> "Auto-declaring a non-durable or auto-delete Exchange ("
						+ exchange.getName()
						+ ") durable:" + exchange.isDurable() + ", auto-delete:" + exchange.isAutoDelete() + ". "
						+ "It will be deleted by the broker if it shuts down, and can be redeclared by closing and "
						+ "reopening the connection.");
			}
		}

		for (Queue queue : queues) {
			if ((!queue.isDurable() || queue.isAutoDelete() || queue.isExclusive())) {
				LOG.info(() -> "Auto-declaring a non-durable, auto-delete, or exclusive Queue ("
						+ queue.getName()
						+ ") durable:" + queue.isDurable() + ", auto-delete:" + queue.isAutoDelete() + ", exclusive:"
						+ queue.isExclusive() + ". "
						+ "It will be redeclared if the broker stops and is restarted while the connection factory is "
						+ "alive, but all messages will be lost.");
			}
		}

		if (exchanges.isEmpty() && queues.isEmpty() && bindings.isEmpty()) {
			LOG.debug("Nothing to declare");
			return;
		}

		try (Management management = getManagement()) {
			exchanges.forEach((exchange) -> doDeclareExchange(management, exchange));
			queues.forEach((queue) -> doDeclareQueue(management, queue));
			bindings.forEach((binding) -> doDeclareBinding(management, binding));
		}

		LOG.debug("Declarations finished");
	}

	/**
	 * Remove any instances that should not be declared by this admin.
	 * @param declarables the collection of {@link Declarable}s.
	 * @param customizers a collection if {@link DeclarableCustomizer} beans.
	 * @param <T> the declarable type.
	 * @return a new collection containing {@link Declarable}s that should be declared by this
	 * admin.
	 */
	@SuppressWarnings({"unchecked", "NullAway"}) // Dataflow analysis limitation
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
				|| dec.getDeclaringAdmins().contains(this.beanName);
	}

	@Override
	public void declareExchange(Exchange exchange) {
		try (Management management = getManagement()) {
			doDeclareExchange(management, exchange);
		}
	}

	private void doDeclareExchange(Management management, Exchange exchange) {
		Management.ExchangeSpecification exchangeSpecification =
				management.exchange(exchange.getName())
						.type(exchange.isDelayed() ? RabbitAdmin.DELAYED_MESSAGE_EXCHANGE : exchange.getType())
//						.internal(exchange.isInternal())
						.arguments(exchange.getArguments())
						.autoDelete(exchange.isAutoDelete());

		if (exchange.isDelayed()) {
			exchangeSpecification.argument("x-delayed-type", exchange.getType());
		}
		try {
			exchangeSpecification.declare();
		}
		catch (AmqpException ex) {
			logOrRethrowDeclarationException(exchange, "exchange", ex);
		}
	}

	@Override
	@ManagedOperation(description = "Delete an exchange from the broker")
	public boolean deleteExchange(String exchangeName) {
		if (isDeletingDefaultExchange(exchangeName)) {
			return false;
		}

		try (Management management = getManagement()) {
			management.exchangeDelete(exchangeName);
		}
		return true;
	}

	@Override
	public @Nullable Queue declareQueue() {
		try (Management management = getManagement()) {
			return doDeclareQueue(management);
		}
	}

	private @Nullable Queue doDeclareQueue(Management management) {
		try {
			Management.QueueInfo queueInfo =
					management.queue()
							.autoDelete(true)
							.exclusive(true)
							.classic()
							.queue()
							.declare();

			return new Queue(queueInfo.name(), false, true, true);
		}
		catch (AmqpException ex) {
			logOrRethrowDeclarationException(null, "queue", ex);
		}
		return null;
	}

	@Override
	public @Nullable String declareQueue(Queue queue) {
		try (Management management = getManagement()) {
			return doDeclareQueue(management, queue);
		}
	}

	private @Nullable String doDeclareQueue(Management management, Queue queue) {
		Management.QueueSpecification queueSpecification =
				management.queue(queue.getName())
						.autoDelete(queue.isAutoDelete())
						.exclusive(queue.isExclusive())
						.arguments(queue.getArguments())
						.classic()
						.queue();

		try {
			String actualName = queueSpecification.declare().name();
			queue.setActualName(actualName);
			return actualName;
		}
		catch (AmqpException ex) {
			logOrRethrowDeclarationException(queue, "queue", ex);
		}
		return null;
	}

	@Override
	@ManagedOperation(description = "Delete a queue from the broker")
	public boolean deleteQueue(String queueName) {
		deleteQueue(queueName, false, false);
		return true;
	}

	@Override
	@ManagedOperation(description =
			"Delete a queue from the broker if unused and empty (when corresponding arguments are true")
	public void deleteQueue(String queueName, boolean unused, boolean empty) {
		try (Management management = getManagement()) {
			Management.QueueInfo queueInfo = management.queueInfo(queueName);
			if ((!unused || queueInfo.consumerCount() == 0)
					&& (!empty || queueInfo.messageCount() == 0)) {

				management.queueDelete(queueName);
			}
		}
	}

	@Override
	@ManagedOperation(description = "Purge a queue and optionally don't wait for the purge to occur")
	public void purgeQueue(String queueName, boolean noWait) {
		if (noWait) {
			this.taskExecutor.execute(() -> purgeQueue(queueName));
		}
		else {
			purgeQueue(queueName);
		}
	}

	@Override
	@ManagedOperation(description = "Purge a queue and return the number of messages purged")
	public int purgeQueue(String queueName) {
		try (Management management = getManagement()) {
			return (int) management.queuePurge(queueName).messageCount();
		}
	}

	@Override
	public void declareBinding(Binding binding) {
		try (Management management = getManagement()) {
			doDeclareBinding(management, binding);
		}
	}

	private void doDeclareBinding(Management management, Binding binding) {
		try {
			Management.BindingSpecification bindingSpecification =
					management.binding()
							.sourceExchange(binding.getExchange())
							.key(binding.getRoutingKey())
							.arguments(binding.getArguments());
			if (binding.isDestinationQueue()) {
				bindingSpecification.destinationQueue(binding.getDestination());
			}
			else {
				bindingSpecification.destinationExchange(binding.getDestination());
			}
			bindingSpecification.bind();
		}
		catch (AmqpException ex) {
			logOrRethrowDeclarationException(binding, "binding", ex);
		}
	}

	@Override
	public void removeBinding(Binding binding) {
		if (binding.isDestinationQueue() && isRemovingImplicitQueueBinding(binding)) {
			return;
		}

		try (Management management = getManagement()) {
			Management.UnbindSpecification unbindSpecification =
					management.unbind()
							.sourceExchange(binding.getExchange())
							.key(binding.getRoutingKey())
							.arguments(binding.getArguments());

			if (binding.isDestinationQueue()) {
				unbindSpecification.destinationQueue(binding.getDestination());
			}
			else {
				unbindSpecification.destinationExchange(binding.getDestination());
			}
			unbindSpecification.unbind();
		}
	}

	/**
	 * Returns 4 properties {@link RabbitAdmin#QUEUE_NAME}, {@link RabbitAdmin#QUEUE_MESSAGE_COUNT},
	 * {@link RabbitAdmin#QUEUE_CONSUMER_COUNT}, {@link #QUEUE_TYPE}, or null if the queue doesn't exist.
	 */
	@Override
	@ManagedOperation(description = "Get queue name, message count and consumer count")
	public @Nullable Properties getQueueProperties(final String queueName) {
		QueueInformation queueInfo = getQueueInfo(queueName);
		if (queueInfo != null) {
			Properties props = new Properties();
			props.put(RabbitAdmin.QUEUE_NAME, queueInfo.getName());
			props.put(RabbitAdmin.QUEUE_MESSAGE_COUNT, queueInfo.getMessageCount());
			props.put(RabbitAdmin.QUEUE_CONSUMER_COUNT, queueInfo.getConsumerCount());
			props.put(QUEUE_TYPE, queueInfo.getType());
			return props;
		}
		else {
			return null;
		}
	}

	@Override
	public @Nullable QueueInformation getQueueInfo(String queueName) {
		try (Management management = getManagement()) {
			Management.QueueInfo queueInfo = management.queueInfo(queueName);
			QueueInformation queueInformation =
					new QueueInformation(queueInfo.name(), queueInfo.messageCount(), queueInfo.consumerCount());
			queueInformation.setType(queueInfo.type().name().toLowerCase());
			return queueInformation;
		}
	}

	private Management getManagement() {
		return this.connectionFactory.getConnection().management();
	}

	private <T extends Throwable> void logOrRethrowDeclarationException(@Nullable Declarable element,
			String elementType, T t) throws T {

		publishDeclarationExceptionEvent(element, t);
		if (this.ignoreDeclarationExceptions || (element != null && element.isIgnoreDeclarationExceptions())) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(t, "Failed to declare " + elementType
						+ ": " + (element == null ? "broker-generated" : element)
						+ ", continuing...");
			}
			else if (LOG.isWarnEnabled()) {
				Throwable cause = t;
				if (t instanceof IOException && t.getCause() != null) {
					cause = t.getCause();
				}
				LOG.warn("Failed to declare " + elementType
						+ ": " + (element == null ? "broker-generated" : element)
						+ ", continuing... " + cause);
			}
		}
		else {
			throw t;
		}
	}

	private void publishDeclarationExceptionEvent(@Nullable Declarable element, Throwable ex) {
		DeclarationExceptionEvent event = new DeclarationExceptionEvent(this, element, ex);
		this.lastDeclarationExceptionEvent = event;
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(event);
		}
	}

	private static boolean isDeletingDefaultExchange(String exchangeName) {
		if (isDefaultExchange(exchangeName)) {
			LOG.warn("Default exchange cannot be deleted.");
			return true;
		}
		return false;
	}

	private static boolean isDefaultExchange(@Nullable String exchangeName) {
		return exchangeName == null || RabbitAdmin.DEFAULT_EXCHANGE_NAME.equals(exchangeName);
	}

	private static boolean isRemovingImplicitQueueBinding(Binding binding) {
		if (isImplicitQueueBinding(binding)) {
			LOG.warn("Cannot remove implicit default exchange binding to queue.");
			return true;
		}
		return false;
	}

	private static boolean isImplicitQueueBinding(Binding binding) {
		return isDefaultExchange(binding.getExchange()) &&
				Objects.equals(binding.getDestination(), binding.getRoutingKey());
	}

	private static void processDeclarables(Collection<Exchange> contextExchanges, Collection<Queue> contextQueues,
			Collection<Binding> contextBindings, Collection<Declarables> declarables) {

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

}
