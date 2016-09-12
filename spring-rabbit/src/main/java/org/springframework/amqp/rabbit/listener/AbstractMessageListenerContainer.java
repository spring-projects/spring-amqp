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

package org.springframework.amqp.rabbit.listener;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;

import org.aopalliance.aop.Advice;

import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.RoutingConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ErrorHandler;
import org.springframework.util.StringUtils;

import com.rabbitmq.client.Channel;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author James Carr
 * @author Gary Russell
 */
public abstract class AbstractMessageListenerContainer extends RabbitAccessor
		implements MessageListenerContainer, ApplicationContextAware, BeanNameAware, DisposableBean,
				ApplicationEventPublisherAware {

	public static final boolean DEFAULT_DEBATCHING_ENABLED = true;

	public static final int DEFAULT_PREFETCH_COUNT = 1;

	/**
	 * The default recovery interval: 5000 ms = 5 seconds.
	 */
	public static final long DEFAULT_RECOVERY_INTERVAL = 5000;

	public static final long DEFAULT_SHUTDOWN_TIMEOUT = 5000;

	private final ContainerDelegate delegate = this::actualInvokeListener;

	protected final Object consumersMonitor = new Object(); //NOSONAR

	private final Map<String, Object> consumerArgs = new HashMap<String, Object>();

	private ContainerDelegate proxy = this.delegate;

	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private ApplicationEventPublisher applicationEventPublisher;

	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute = new DefaultTransactionAttribute();

	private String beanName;

	private Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean taskExecutorSet;

	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE;

	private volatile boolean active = false;

	private volatile boolean running = false;

	private final Object lifecycleMonitor = new Object();

	private volatile List<String> queueNames = new CopyOnWriteArrayList<String>();

	private ErrorHandler errorHandler = new ConditionalRejectingErrorHandler();

	private MessageConverter messageConverter;

	private boolean exposeListenerChannel = true;

	private volatile Object messageListener;

	private volatile AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private volatile boolean deBatchingEnabled = DEFAULT_DEBATCHING_ENABLED;

	private volatile boolean initialized;

	private Collection<MessagePostProcessor> afterReceivePostProcessors;

	private volatile ApplicationContext applicationContext;

	private String listenerId;

	private Advice[] adviceChain = new Advice[0];

	private ConsumerTagStrategy consumerTagStrategy;

	private volatile boolean exclusive;

	private volatile boolean defaultRequeueRejected = true;

	private volatile int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private long idleEventInterval;

	private volatile long lastReceive = System.currentTimeMillis();

	/**
	 * {@inheritDoc}
	 * @since 1.5
	 */
	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
		this.applicationEventPublisher = applicationEventPublisher;
	}

	protected ApplicationEventPublisher getApplicationEventPublisher() {
		return this.applicationEventPublisher;
	}

	/**
	 * <p>
	 * Flag controlling the behaviour of the container with respect to message acknowledgement. The most common usage is
	 * to let the container handle the acknowledgements (so the listener doesn't need to know about the channel or the
	 * message).
	 * <p>
	 * Set to {@link AcknowledgeMode#MANUAL} if the listener will send the acknowledgements itself using
	 * {@link Channel#basicAck(long, boolean)}. Manual acks are consistent with either a transactional or
	 * non-transactional channel, but if you are doing no other work on the channel at the same other than receiving a
	 * single message then the transaction is probably unnecessary.
	 * <p>
	 * Set to {@link AcknowledgeMode#NONE} to tell the broker not to expect any acknowledgements, and it will assume all
	 * messages are acknowledged as soon as they are sent (this is "autoack" in native Rabbit broker terms). If
	 * {@link AcknowledgeMode#NONE} then the channel cannot be transactional (so the container will fail on start up if
	 * that flag is accidentally set).
	 * @param acknowledgeMode the acknowledge mode to set. Defaults to {@link AcknowledgeMode#AUTO}
	 * @see AcknowledgeMode
	 */
	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	/**
	 * @return the acknowledgeMode
	 */
	public AcknowledgeMode getAcknowledgeMode() {
		return this.acknowledgeMode;
	}

	/**
	 * Set the name of the queue(s) to receive messages from.
	 * @param queueName the desired queueName(s) (can not be <code>null</code>)
	 */
	public void setQueueNames(String... queueName) {
		this.queueNames = new CopyOnWriteArrayList<>(Arrays.asList(queueName));
	}

	/**
	 * Set the name of the queue(s) to receive messages from.
	 * @param queues the desired queue(s) (can not be <code>null</code>)
	 */
	public void setQueues(Queue... queues) {
		List<String> queueNames = new ArrayList<String>(queues.length);
		for (int i = 0; i < queues.length; i++) {
			Assert.notNull(queues[i], "Queue must not be null.");
			queueNames.add(queues[i].getName());
		}
		queueNames = new CopyOnWriteArrayList<>(queueNames);
		this.queueNames = queueNames;
	}

	/**
	 * @return the name of the queues to receive messages from.
	 */
	public String[] getQueueNames() {
		return this.queueNames.toArray(new String[this.queueNames.size()]);
	}

	protected String[] getRequiredQueueNames() {
		Assert.state(this.queueNames.size() > 0, "Queue names must not be empty.");
		return this.getQueueNames();
	}

	protected Set<String> getQueueNamesAsSet() {
		return new HashSet<String>(this.queueNames);
	}

	/**
	 * Add queue(s) to this container's list of queues.
	 * @param queueNames The queue(s) to add.
	 */
	public void addQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' cannot be null");
		Assert.noNullElements(queueNames, "'queueNames' cannot contain null elements");
		this.queueNames.addAll(Arrays.asList(queueNames));
	}

	/**
	 * Add queue(s) to this container's list of queues.
	 * @param queues The queue(s) to add.
	 */
	public void addQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' cannot be null");
		Assert.noNullElements(queues, "'queues' cannot contain null elements");
		String[] queueNames = new String[queues.length];
		for (int i = 0; i < queues.length; i++) {
			queueNames[i] = queues[i].getName();
		}
		this.addQueueNames(queueNames);
	}

	/**
	 * Remove queue(s) from this container's list of queues.
	 * @param queueNames The queue(s) to remove.
	 * @return the boolean result of removal on the target {@code queueNames} List.
	 */
	public boolean removeQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' cannot be null");
		Assert.noNullElements(queueNames, "'queueNames' cannot contain null elements");
		Assert.isTrue(canRemoveLastQueue() || this.queueNames.size() - queueNames.length > 0,
				"Cannot remove the last queue");
		return this.queueNames.removeAll(Arrays.asList(queueNames));
	}

	/**
	 * Subclasses can override this method if they allow running with zero configured
	 * queues.
	 * @return true to allow removal of the last queue.
	 * @since 2.0
	 */
	protected boolean canRemoveLastQueue() {
		return false;
	}

	/**
	 * Remove queue(s) from this container's list of queues.
	 * @param queues The queue(s) to remove.
	 * @return the boolean result of removal on the target {@code queueNames} List.
	 */
	public boolean removeQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' cannot be null");
		Assert.noNullElements(queues, "'queues' cannot contain null elements");
		String[] queueNames = new String[queues.length];
		for (int i = 0; i < queues.length; i++) {
			queueNames[i] = queues[i].getName();
		}
		return this.removeQueueNames(queueNames);
	}

	/**
	 * @return whether to expose the listener {@link Channel} to a registered {@link ChannelAwareMessageListener}.
	 */
	public boolean isExposeListenerChannel() {
		return this.exposeListenerChannel;
	}

	/**
	 * Set whether to expose the listener Rabbit Channel to a registered {@link ChannelAwareMessageListener} as well as
	 * to {@link org.springframework.amqp.rabbit.core.RabbitTemplate} calls.
	 * <p>
	 * Default is "true", reusing the listener's {@link Channel}. Turn this off to expose a fresh Rabbit Channel fetched
	 * from the same underlying Rabbit {@link Connection} instead.
	 * <p>
	 * Note that Channels managed by an external transaction manager will always get exposed to
	 * {@link org.springframework.amqp.rabbit.core.RabbitTemplate} calls. So in terms of RabbitTemplate exposure, this
	 * setting only affects locally transacted Channels.
	 *
	 * @param exposeListenerChannel true to expose the channel.
	 *
	 * @see ChannelAwareMessageListener
	 */
	public void setExposeListenerChannel(boolean exposeListenerChannel) {
		this.exposeListenerChannel = exposeListenerChannel;
	}

	/**
	 * Set the message listener implementation to register. This can be either a Spring
	 * {@link MessageListener} object or a Spring {@link ChannelAwareMessageListener}
	 * object. Using the strongly typed
	 * {@link #setChannelAwareMessageListener(ChannelAwareMessageListener)} is preferred.
	 *
	 * @param messageListener The listener.
	 * @throws IllegalArgumentException if the supplied listener is not a
	 * {@link MessageListener} or a {@link ChannelAwareMessageListener}
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	public void setMessageListener(Object messageListener) {
		checkMessageListener(messageListener);
		this.messageListener = messageListener;
	}

	/**
	 * Set the {@link MessageListener}; strongly typed version of
	 * {@link #setMessageListener(Object)}.
	 * @param messageListener the listener.
	 * @since 2.0
	 */
	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * Set the {@link ChannelAwareMessageListener}; strongly typed version of
	 * {@link #setMessageListener(Object)}.
	 * @param messageListener the listener.
	 * @since 2.0
	 */
	public void setChannelAwareMessageListener(ChannelAwareMessageListener messageListener) {
		this.messageListener = messageListener;
	}

	/**
	 * Check the given message listener, throwing an exception if it does not correspond to a supported listener type.
	 * <p>
	 * By default, only a Spring {@link MessageListener} object or a Spring
	 * {@link ChannelAwareMessageListener} object will be accepted.
	 * @param messageListener the message listener object to check
	 * @throws IllegalArgumentException if the supplied listener is not a MessageListener or SessionAwareMessageListener
	 * @see MessageListener
	 * @see ChannelAwareMessageListener
	 */
	protected void checkMessageListener(Object messageListener) {
		if (!(messageListener instanceof MessageListener || messageListener instanceof ChannelAwareMessageListener)) {
			throw new IllegalArgumentException("Message listener needs to be of type ["
					+ MessageListener.class.getName() + "] or [" + ChannelAwareMessageListener.class.getName() + "]");
		}
	}

	/**
	 * @return The message listener object to register.
	 */
	public Object getMessageListener() {
		return this.messageListener;
	}

	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown while processing a Message. By
	 * default there will be <b>no</b> ErrorHandler so that error-level logging is the only result.
	 *
	 * @param errorHandler The error handler.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Set the {@link MessageConverter} strategy for converting AMQP Messages.
	 * @param messageConverter the message converter to use
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Determine whether or not the container should de-batch batched
	 * messages (true) or call the listener with the batch (false). Default: true.
	 * @param deBatchingEnabled the deBatchingEnabled to set.
	 */
	public void setDeBatchingEnabled(boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	/**
	 * Public setter for the {@link Advice} to apply to listener executions.
	 * <p>
	 * If a {code #setTransactionManager(PlatformTransactionManager) transactionManager} is provided as well, then
	 * separate advice is created for the transaction and applied first in the chain. In that case the advice chain
	 * provided here should not contain a transaction interceptor (otherwise two transactions would be be applied).
	 * @param adviceChain the advice chain to set
	 */
	public void setAdviceChain(Advice... adviceChain) {
		this.adviceChain = Arrays.copyOf(adviceChain, adviceChain.length);
	}

	protected Advice[] getAdviceChain() {
		return this.adviceChain;
	}

	/**
	 * Set {@link MessagePostProcessor}s that will be applied after message reception, before
	 * invoking the {@link MessageListener}. Often used to decompress data.  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post processor.
	 * @since 1.4.2
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		Assert.notNull(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot be null");
		Assert.noNullElements(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot have null elements");
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(afterReceivePostProcessors));
	}

	/**
	 * Set whether to automatically start the container after initialization.
	 * <p>
	 * Default is "true"; set this to "false" to allow for manual startup through the {@link #start()} method.
	 *
	 * @param autoStartup true for auto startup.
	 */
	public void setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Specify the phase in which this container should be started and stopped. The startup order proceeds from lowest
	 * to highest, and the shutdown order is the reverse of that. By default this value is Integer.MAX_VALUE meaning
	 * that this container starts as late as possible and stops as soon as possible.
	 *
	 * @param phase The phase.
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}

	/**
	 * @return The phase in which this container will be started and stopped.
	 */
	@Override
	public int getPhase() {
		return this.phase;
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	/**
	 * @return The bean name that this listener container has been assigned in its containing bean factory, if any.
	 */
	protected final String getBeanName() {
		return this.beanName;
	}

	protected final ApplicationContext getApplicationContext() {
		return this.applicationContext;
	}

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	@Override
	public ConnectionFactory getConnectionFactory() {
		ConnectionFactory connectionFactory = super.getConnectionFactory();
		if (connectionFactory instanceof RoutingConnectionFactory) {
			ConnectionFactory targetConnectionFactory = ((RoutingConnectionFactory) connectionFactory)
					.getTargetConnectionFactory(this.queueNames.toString().replaceAll(" ", ""));
			if (targetConnectionFactory != null) {
				return targetConnectionFactory;
			}
		}
		return connectionFactory;
	}

	/**
	 * The 'id' attribute of the listener.
	 * @return the id (or the container bean name if no id set).
	 */
	public String getListenerId() {
		return this.listenerId != null ? this.listenerId : this.beanName;
	}

	public void setListenerId(String listenerId) {
		this.listenerId = listenerId;
	}

	/**
	 * Set the implementation of {@link ConsumerTagStrategy} to generate consumer tags.
	 * By default, the RabbitMQ server generates consumer tags.
	 * @param consumerTagStrategy the consumerTagStrategy to set.
	 * @since 1.4.5
	 */
	public void setConsumerTagStrategy(ConsumerTagStrategy consumerTagStrategy) {
		this.consumerTagStrategy = consumerTagStrategy;
	}

	/**
	 * Return the consumer tag strategy to use.
	 * @return the strategy.
	 * @since 2.0
	 */
	protected ConsumerTagStrategy getConsumerTagStrategy() {
		return this.consumerTagStrategy;
	}

	/**
	 * Set consumer arguments.
	 * @param args the arguments.
	 * @since 1.3
	 */
	public void setConsumerArguments(Map<String, Object> args) {
		synchronized (this.consumersMonitor) {
			this.consumerArgs.clear();
			this.consumerArgs.putAll(args);
		}
	}

	/**
	 * Return the consumer arguments.
	 * @return the arguments.
	 * @since 2.0
	 */
	protected Map<String, Object> getConsumerArguments() {
		return this.consumerArgs;
	}

	/**
	 * Set to true for an exclusive consumer.
	 * @param exclusive true for an exclusive consumer.
	 */
	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	/**
	 * Return whether the consumers should be exclusive.
	 * @return true for exclusive consumers.
	 */
	protected boolean isExclusive() {
		return this.exclusive;
	}

	/**
	 * Set the default behavior when a message is rejected, for example because the listener
	 * threw an exception. When true, messages will be requeued, when false, they will not. For
	 * versions of Rabbit that support dead-lettering, the message must not be requeued in order
	 * to be sent to the dead letter exchange. Setting to false causes all rejections to not
	 * be requeued. When true, the default can be overridden by the listener throwing an
	 * {@link AmqpRejectAndDontRequeueException}. Default true.
	 * @param defaultRequeueRejected true to reject by default.
	 */
	public void setDefaultRequeueRejected(boolean defaultRequeueRejected) {
		this.defaultRequeueRejected = defaultRequeueRejected;
	}

	/**
	 * Return the default requeue rejected.
	 * @return the boolean.
	 * @see #setDefaultRequeueRejected(boolean)
	 * @since 2.0
	 */
	protected boolean isDefaultRequeueRejected() {
		return this.defaultRequeueRejected;
	}

	/**
	 * Tell the broker how many messages to send to each consumer in a single request.
	 * Often this can be set quite high to improve throughput.
	 * @param prefetchCount the prefetch count
	 */
	public void setPrefetchCount(int prefetchCount) {
		this.prefetchCount = prefetchCount;
	}

	/**
	 * Return the prefetch count.
	 * @return the count.
	 * @since 2.0
	 */
	protected int getPrefetchCount() {
		return this.prefetchCount;
	}

	/**
	 * The time to wait for workers in milliseconds after the container is stopped. If any
	 * workers are active when the shutdown signal comes they will be allowed to finish
	 * processing as long as they can finish within this timeout. Defaults
	 * to 5 seconds.
	 * @param shutdownTimeout the shutdown timeout to set
	 */
	public void setShutdownTimeout(long shutdownTimeout) {
		this.shutdownTimeout = shutdownTimeout;
	}

	protected long getShutdownTimeout() {
		return this.shutdownTimeout;
	}

	/**
	 * How often to emit {@link ListenerContainerIdleEvent}s in milliseconds.
	 * @param idleEventInterval the interval.
	 */
	public void setIdleEventInterval(long idleEventInterval) {
		this.idleEventInterval = idleEventInterval;
	}

	protected long getIdleEventInterval() {
		return this.idleEventInterval;
	}

	/**
	 * Get the time the last message was received - initialized to container start
	 * time.
	 * @return the time.
	 */
	protected long getLastReceive() {
		return this.lastReceive;
	}

	/**
	 * Set the transaction manager to use.
	 * @param transactionManager the transaction manager.
	 */
	public void setTransactionManager(PlatformTransactionManager transactionManager) {
		this.transactionManager = transactionManager;
	}

	protected PlatformTransactionManager getTransactionManager() {
		return this.transactionManager;
	}

	/**
	 * Set the transaction attribute to use when using an external transaction manager.
	 * @param transactionAttribute the transaction attribute to set
	 */
	public void setTransactionAttribute(TransactionAttribute transactionAttribute) {
		this.transactionAttribute = transactionAttribute;
	}

	protected TransactionAttribute getTransactionAttribute() {
		return this.transactionAttribute;
	}

	/**
	 * Set a task executor for the container - used to create the consumers not at
	 * runtime.
	 * @param taskExecutor the task executor.
	 */
	public void setTaskExecutor(Executor taskExecutor) {
		this.taskExecutor = taskExecutor;
		this.taskExecutorSet = true;
	}

	protected Executor getTaskExecutor() {
		return this.taskExecutor;
	}

	/**
	 * Delegates to {@link #validateConfiguration()} and {@link #initialize()}.
	 */
	@Override
	public final void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.state(
				this.exposeListenerChannel || !getAcknowledgeMode().isManual(),
				"You cannot acknowledge messages manually if the channel is not exposed to the listener "
						+ "(please check your configuration and set exposeListenerChannel=true or acknowledgeMode!=MANUAL)");
		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && isChannelTransacted()),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having a "
						+ "transactional channel. Either use a different AcknowledgeMode or make sure channelTransacted=false");
		validateConfiguration();
		initialize();
	}

	@Override
	public void setupMessageListener(Object messageListener) {
		setMessageListener(messageListener);
	}

	/**
	 * Validate the configuration of this container.
	 * <p>
	 * The default implementation is empty. To be overridden in subclasses.
	 */
	protected void validateConfiguration() {
	}

	protected void initializeProxy(Object delegate) {
		if (this.getAdviceChain().length == 0) {
			return;
		}
		ProxyFactory factory = new ProxyFactory();
		for (Advice advice : getAdviceChain()) {
			factory.addAdvisor(new DefaultPointcutAdvisor(advice));
		}
		factory.addInterface(ContainerDelegate.class);
		factory.setTarget(delegate);
		this.proxy = (ContainerDelegate) factory.getProxy(ContainerDelegate.class.getClassLoader());
	}

	/**
	 * Calls {@link #shutdown()} when the BeanFactory destroys the container instance.
	 * @see #shutdown()
	 */
	@Override
	public void destroy() {
		shutdown();
	}

	// -------------------------------------------------------------------------
	// Lifecycle methods for starting and stopping the container
	// -------------------------------------------------------------------------

	/**
	 * Initialize this container.
	 * <p>
	 * Creates a Rabbit Connection and calls {@link #doInitialize()}.
	 */
	public void initialize() {
		try {
			synchronized (this.lifecycleMonitor) {
				this.lifecycleMonitor.notifyAll();
			}
			initializeProxy(this.delegate);
			doInitialize();
			if (!this.isExposeListenerChannel() && this.transactionManager != null) {
				logger.warn("exposeListenerChannel=false is ignored when using a TransactionManager");
			}
			if (!this.taskExecutorSet && StringUtils.hasText(this.getBeanName())) {
				this.taskExecutor = new SimpleAsyncTaskExecutor(this.getBeanName() + "-");
				this.taskExecutorSet = true;
			}
			if (this.transactionManager != null) {
				if (!isChannelTransacted()) {
					logger.debug("The 'channelTransacted' is coerced to 'true', when 'transactionManager' is provided");
					setChannelTransacted(true);
				}

			}
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	/**
	 * Stop the shared Connection, call {@link #doShutdown()}, and close this container.
	 */
	public void shutdown() {
		logger.debug("Shutting down Rabbit listener container");
		synchronized (this.lifecycleMonitor) {
			this.active = false;
			this.lifecycleMonitor.notifyAll();
		}

		// Shut down the invokers.
		try {
			doShutdown();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			synchronized (this.lifecycleMonitor) {
				this.running = false;
				this.lifecycleMonitor.notifyAll();
			}
		}
	}

	/**
	 * Register any invokers within this container.
	 * <p>
	 * Subclasses need to implement this method for their specific invoker management process.
	 *
	 * @throws Exception Any Exception.
	 */
	protected abstract void doInitialize() throws Exception;

	/**
	 * Close the registered invokers.
	 * <p>
	 * Subclasses need to implement this method for their specific invoker management process.
	 * <p>
	 * A shared Rabbit Connection, if any, will automatically be closed <i>afterwards</i>.
	 * @see #shutdown()
	 */
	protected abstract void doShutdown();

	/**
	 * @return Whether this container is currently active, that is, whether it has been set up but not shut down yet.
	 */
	public final boolean isActive() {
		synchronized (this.lifecycleMonitor) {
			return this.active;
		}
	}

	/**
	 * Start this container.
	 * @see #doStart
	 */
	@Override
	public void start() {
		if (!this.initialized) {
			synchronized (this.lifecycleMonitor) {
				if (!this.initialized) {
					afterPropertiesSet();
					this.initialized = true;
				}
			}
		}
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("Starting Rabbit listener container.");
			}
			doStart();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	/**
	 * Start this container, and notify all invoker tasks.
	 * @throws Exception if thrown by Rabbit API methods
	 */
	protected void doStart() throws Exception {
		// Reschedule paused tasks, if any.
		synchronized (this.lifecycleMonitor) {
			this.active = true;
			this.running = true;
			this.lifecycleMonitor.notifyAll();
		}
	}

	/**
	 * Stop this container.
	 * @see #doStop
	 * @see #doStop
	 */
	@Override
	public void stop() {
		try {
			doStop();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			synchronized (this.lifecycleMonitor) {
				this.running = false;
				this.lifecycleMonitor.notifyAll();
			}
		}
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	/**
	 * This method is invoked when the container is stopping.
	 */
	protected void doStop() {
		shutdown();
	}

	/**
	 * Determine whether this container is currently running, that is, whether it has been started and not stopped yet.
	 * @see #start()
	 * @see #stop()
	 */
	@Override
	public final boolean isRunning() {
		synchronized (this.lifecycleMonitor) {
			return (this.running);
		}
	}

	/**
	 * Invoke the registered ErrorHandler, if any. Log at error level otherwise.
	 * The default error handler is a {@link ConditionalRejectingErrorHandler} with
	 * the default {@link FatalExceptionStrategy} implementation.
	 * @param ex the uncaught error that arose during Rabbit processing.
	 * @see #setErrorHandler
	 */
	protected void invokeErrorHandler(Throwable ex) {
		if (this.errorHandler != null) {
			this.errorHandler.handleError(ex);
		}
		else if (logger.isWarnEnabled()) {
			logger.warn("Execution of Rabbit message listener failed, and no ErrorHandler has been set.", ex);
		}
	}

	// -------------------------------------------------------------------------
	// Template methods for listener execution
	// -------------------------------------------------------------------------

	/**
	 * Execute the specified listener, committing or rolling back the transaction afterwards (if necessary).
	 *
	 * @param channel the Rabbit Channel to operate on
	 * @param messageIn the received Rabbit Message
	 * @throws Exception Any Exception.
	 *
	 * @see #invokeListener
	 * @see #handleListenerException
	 */
	protected void executeListener(Channel channel, Message messageIn) throws Exception {
		if (!isRunning()) {
			if (logger.isWarnEnabled()) {
				logger.warn("Rejecting received message because the listener container has been stopped: " + messageIn);
			}
			throw new MessageRejectedWhileStoppingException();
		}
		try {
			Message message = messageIn;
			if (this.afterReceivePostProcessors != null) {
				for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
					message = processor.postProcessMessage(message);
				}
			}
			Object batchFormat = message.getMessageProperties().getHeaders().get(MessageProperties.SPRING_BATCH_FORMAT);
			if (MessageProperties.BATCH_FORMAT_LENGTH_HEADER4.equals(batchFormat) && this.deBatchingEnabled) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(message.getBody());
				MessageProperties messageProperties = message.getMessageProperties();
				messageProperties.getHeaders().remove(MessageProperties.SPRING_BATCH_FORMAT);
				while (byteBuffer.hasRemaining()) {
					int length = byteBuffer.getInt();
					if (length < 0 || length > byteBuffer.remaining()) {
						throw new ListenerExecutionFailedException("Bad batched message received",
								new MessageConversionException("Insufficient batch data at offset " + byteBuffer.position()),
										message);
					}
					byte[] body = new byte[length];
					byteBuffer.get(body);
					messageProperties.setContentLength(length);
					// Caveat - shared MessageProperties.
					Message fragment = new Message(body, messageProperties);
					invokeListener(channel, fragment);
				}
			}
			else {
				invokeListener(channel, message);
			}
		}
		catch (Exception ex) {
			handleListenerException(ex);
			throw ex;
		}
	}

	protected void invokeListener(Channel channel, Message message) throws Exception {
		this.proxy.invokeListener(channel, message);
	}

	/**
	 * Invoke the specified listener: either as standard MessageListener or (preferably) as SessionAwareMessageListener.
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Exception if thrown by Rabbit API methods
	 * @see #setMessageListener
	 */
	protected void actualInvokeListener(Channel channel, Message message) throws Exception {
		Object listener = getMessageListener();
		if (listener instanceof ChannelAwareMessageListener) {
			doInvokeListener((ChannelAwareMessageListener) listener, channel, message);
		}
		else if (listener instanceof MessageListener) {
			boolean bindChannel = isExposeListenerChannel() && isChannelLocallyTransacted();
			if (bindChannel) {
				RabbitResourceHolder resourceHolder = new RabbitResourceHolder(channel, false);
				resourceHolder.setSynchronizedWithTransaction(true);
				TransactionSynchronizationManager.bindResource(this.getConnectionFactory(),
						resourceHolder);
			}
			try {
				doInvokeListener((MessageListener) listener, message);
			}
			finally {
				if (bindChannel) {
					// unbind if we bound
					TransactionSynchronizationManager.unbindResource(this.getConnectionFactory());
				}
			}
		}
		else if (listener != null) {
			throw new FatalListenerExecutionException("Only MessageListener and SessionAwareMessageListener supported: "
					+ listener);
		}
		else {
			throw new FatalListenerExecutionException("No message listener specified - see property 'messageListener'");
		}
	}

	/**
	 * Invoke the specified listener as Spring ChannelAwareMessageListener, exposing a new Rabbit Session (potentially
	 * with its own transaction) to the listener if demanded.
	 * @param listener the Spring ChannelAwareMessageListener to invoke
	 * @param channel the Rabbit Channel to operate on
	 * @param message the received Rabbit Message
	 * @throws Exception if thrown by Rabbit API methods or listener itself.
	 * <p>
	 * Exception thrown from listener will be wrapped to {@link ListenerExecutionFailedException}.
	 * @see ChannelAwareMessageListener
	 * @see #setExposeListenerChannel(boolean)
	 */
	protected void doInvokeListener(ChannelAwareMessageListener listener, Channel channel, Message message)
			throws Exception {

		RabbitResourceHolder resourceHolder = null;
		Channel channelToUse = channel;
		boolean boundHere = false;
		try {
			if (!isExposeListenerChannel()) {
				// We need to expose a separate Channel.
				resourceHolder = getTransactionalResourceHolder();
				channelToUse = resourceHolder.getChannel();
				/*
				 * If there is a real transaction, the resource will have been bound; otherwise
				 * we need to bind it temporarily here. Any work done on this channel
				 * will be committed in the finally block.
				 */
				if (isChannelLocallyTransacted() &&
							!TransactionSynchronizationManager.isActualTransactionActive()) {
						resourceHolder.setSynchronizedWithTransaction(true);
						TransactionSynchronizationManager.bindResource(this.getConnectionFactory(),
								resourceHolder);
					boundHere = true;
				}
			}
			else {
				// if locally transacted, bind the current channel to make it available to RabbitTemplate
				if (isChannelLocallyTransacted()) {
					RabbitResourceHolder localResourceHolder = new RabbitResourceHolder(channelToUse, false);
					localResourceHolder.setSynchronizedWithTransaction(true);
					TransactionSynchronizationManager.bindResource(this.getConnectionFactory(),
							localResourceHolder);
					boundHere = true;
				}
			}
			// Actually invoke the message listener...
			try {
				listener.onMessage(message, channelToUse);
			}
			catch (Exception e) {
				throw wrapToListenerExecutionFailedExceptionIfNeeded(e, message);
			}
		}
		finally {
			if (resourceHolder != null && boundHere) {
				// so the channel exposed (because exposeListenerChannel is false) will be closed
				resourceHolder.setSynchronizedWithTransaction(false);
			}
			ConnectionFactoryUtils.releaseResources(resourceHolder);
			if (boundHere) {
				// unbind if we bound
				TransactionSynchronizationManager.unbindResource(this.getConnectionFactory());
				if (!isExposeListenerChannel() && isChannelLocallyTransacted()) {
					/*
					 *  commit the temporary channel we exposed; the consumer's channel
					 *  will be committed later. Note that when exposing a different channel
					 *  when there's no transaction manager, the exposed channel is committed
					 *  on each message, and not based on txSize.
					 */
					RabbitUtils.commitIfNecessary(channelToUse);
				}
			}
		}
	}

	/**
	 * Invoke the specified listener as Spring Rabbit MessageListener.
	 * <p>
	 * Default implementation performs a plain invocation of the <code>onMessage</code> method.
	 * <p>
	 * Exception thrown from listener will be wrapped to {@link ListenerExecutionFailedException}.
	 *
	 * @param listener the Rabbit MessageListener to invoke
	 * @param message the received Rabbit Message
	 * @throws Exception Any Exception.
	 *
	 * @see org.springframework.amqp.core.MessageListener#onMessage
	 */
	protected void doInvokeListener(MessageListener listener, Message message) throws Exception {
		try {
			listener.onMessage(message);
		}
		catch (Exception e) {
			throw wrapToListenerExecutionFailedExceptionIfNeeded(e, message);
		}
	}

	/**
	 * Check whether the given Channel is locally transacted, that is, whether its transaction is managed by this
	 * listener container's Channel handling and not by an external transaction coordinator.
	 * <p>
	 * Note:This method is about finding out whether the Channel's transaction is local or externally coordinated.
	 * @return whether the given Channel is locally transacted
	 * @see #isChannelTransacted()
	 */
	protected boolean isChannelLocallyTransacted() {
		return this.isChannelTransacted() && this.transactionManager == null;
	}

	/**
	 * Handle the given exception that arose during listener execution.
	 * <p>
	 * The default implementation logs the exception at error level, not propagating it to the Rabbit provider -
	 * assuming that all handling of acknowledgment and/or transactions is done by this listener container. This can be
	 * overridden in subclasses.
	 * @param ex the exception to handle
	 */
	protected void handleListenerException(Throwable ex) {
		if (isActive()) {
			// Regular case: failed while active.
			// Invoke ErrorHandler if available.
			invokeErrorHandler(ex);
		}
		else {
			// Rare case: listener thread failed after container shutdown.
			// Log at debug level, to avoid spamming the shutdown log.
			logger.debug("Listener exception after container shutdown", ex);
		}
	}

	/**
	 * @param e The Exception.
	 * @param message The failed message.
	 * @return If 'e' is of type {@link ListenerExecutionFailedException} - return 'e' as it is, otherwise wrap it to
	 * {@link ListenerExecutionFailedException} and return.
	 */
	protected Exception wrapToListenerExecutionFailedExceptionIfNeeded(Exception e, Message message) {
		if (!(e instanceof ListenerExecutionFailedException)) {
			// Wrap exception to ListenerExecutionFailedException.
			return new ListenerExecutionFailedException("Listener threw exception", e, message);
		}
		return e;
	}

	protected final void publishConsumerFailedEvent(String reason, boolean fatal, Throwable t) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher
					.publishEvent(new ListenerContainerConsumerFailedEvent(this, reason, t, fatal));
		}
	}

	protected final void publishIdleContainerEvent(long idleTime) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher.publishEvent(
					new ListenerContainerIdleEvent(this, idleTime, getListenerId(), getQueueNames()));
		}
	}

	protected void updateLastReceive() {
		if (this.idleEventInterval > 0) {
			this.lastReceive = System.currentTimeMillis();
		}
	}

	@FunctionalInterface
	private interface ContainerDelegate {

		void invokeListener(Channel channel, Message message) throws Exception;

	}

	/**
	 * Exception that indicates that the initial setup of this container's shared Rabbit Connection failed. This is
	 * indicating to invokers that they need to establish the shared Connection themselves on first access.
	 */
	@SuppressWarnings("serial")
	public static class SharedConnectionNotInitializedException extends RuntimeException {

		/**
		 * Create a new SharedConnectionNotInitializedException.
		 * @param msg the detail message
		 */
		protected SharedConnectionNotInitializedException(String msg) {
			super(msg);
		}
	}

	@SuppressWarnings("serial")
	protected static final class WrappedTransactionException extends RuntimeException {

		protected WrappedTransactionException(Throwable cause) {
			super(cause);
		}

	}

}
