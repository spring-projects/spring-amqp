/*
 * Copyright 2002-2022 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.AmqpConnectException;
import org.springframework.amqp.AmqpIOException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.ImmediateAcknowledgeAmqpException;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BatchMessageListener;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactoryUtils;
import org.springframework.amqp.rabbit.connection.RabbitAccessor;
import org.springframework.amqp.rabbit.connection.RabbitResourceHolder;
import org.springframework.amqp.rabbit.connection.RabbitUtils;
import org.springframework.amqp.rabbit.connection.RoutingConnectionFactory;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareBatchMessageListener;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerExecutionException;
import org.springframework.amqp.rabbit.listener.exception.FatalListenerStartupException;
import org.springframework.amqp.rabbit.listener.exception.MessageRejectedWhileStoppingException;
import org.springframework.amqp.rabbit.listener.support.ContainerUtils;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.micrometer.MessageReceiverContext;
import org.springframework.amqp.rabbit.support.micrometer.RabbitListenerObservation;
import org.springframework.amqp.rabbit.support.micrometer.RabbitListenerObservationConvention;
import org.springframework.amqp.support.ConditionalExceptionLogger;
import org.springframework.amqp.support.ConsumerTagStrategy;
import org.springframework.amqp.support.postprocessor.MessagePostProcessorUtils;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.lang.Nullable;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ErrorHandler;
import org.springframework.util.StringUtils;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ShutdownSignalException;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Dave Syer
 * @author James Carr
 * @author Gary Russell
 * @author Alex Panchenko
 * @author Johno Crawford
 * @author Arnaud Cogoluègnes
 * @author Artem Bilan
 * @author Mohammad Hewedy
 * @author Mat Jaggard
 */
public abstract class AbstractMessageListenerContainer extends RabbitAccessor
		implements MessageListenerContainer, ApplicationContextAware, BeanNameAware, DisposableBean,
		ApplicationEventPublisherAware {

	private static final int EXIT_99 = 99;

	private static final String UNCHECKED = "unchecked";

	static final int DEFAULT_FAILED_DECLARATION_RETRY_INTERVAL = 5000;

	public static final boolean DEFAULT_DEBATCHING_ENABLED = true;

	public static final int DEFAULT_PREFETCH_COUNT = 250;

	/**
	 * The default recovery interval: 5000 ms = 5 seconds.
	 */
	public static final long DEFAULT_RECOVERY_INTERVAL = 5000;

	public static final long DEFAULT_SHUTDOWN_TIMEOUT = 5000;

	private static final boolean MICROMETER_PRESENT = ClassUtils.isPresent(
			"io.micrometer.core.instrument.MeterRegistry", AbstractMessageListenerContainer.class.getClassLoader());

	private final Object lifecycleMonitor = new Object();

	private final ContainerDelegate delegate = this::actualInvokeListener;

	protected final Object consumersMonitor = new Object(); //NOSONAR

	private final Map<String, Object> consumerArgs = new HashMap<>();

	private final Map<String, String> micrometerTags = new HashMap<>();

	private ContainerDelegate proxy = this.delegate;

	private long shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

	private ApplicationEventPublisher applicationEventPublisher;

	@Nullable
	private PlatformTransactionManager transactionManager;

	private TransactionAttribute transactionAttribute = new DefaultTransactionAttribute();

	@Nullable
	private String beanName = "not.a.Spring.bean";

	private Executor taskExecutor = new SimpleAsyncTaskExecutor();

	private boolean taskExecutorSet;

	private BackOff recoveryBackOff = new FixedBackOff(DEFAULT_RECOVERY_INTERVAL, FixedBackOff.UNLIMITED_ATTEMPTS);

	private MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();

	private AmqpAdmin amqpAdmin;

	private boolean missingQueuesFatal = true;

	private boolean missingQueuesFatalSet;

	private boolean possibleAuthenticationFailureFatal = true;

	private boolean possibleAuthenticationFailureFatalSet;

	private boolean autoDeclare = true;

	private boolean mismatchedQueuesFatal = false;

	private long failedDeclarationRetryInterval = DEFAULT_FAILED_DECLARATION_RETRY_INTERVAL;

	private boolean autoStartup = true;

	private int phase = Integer.MAX_VALUE;

	private boolean active = false;

	private boolean running = false;

	private ErrorHandler errorHandler = new ConditionalRejectingErrorHandler();

	private boolean exposeListenerChannel = true;

	private MessageListener messageListener;

	private AcknowledgeMode acknowledgeMode = AcknowledgeMode.AUTO;

	private boolean deBatchingEnabled = DEFAULT_DEBATCHING_ENABLED;

	private boolean initialized;

	private Collection<MessagePostProcessor> afterReceivePostProcessors;

	private ApplicationContext applicationContext;

	private String listenerId;

	private Advice[] adviceChain = new Advice[0];

	@Nullable
	private ConsumerTagStrategy consumerTagStrategy;

	private boolean exclusive;

	private boolean noLocal;

	private boolean defaultRequeueRejected = true;

	private int prefetchCount = DEFAULT_PREFETCH_COUNT;

	private boolean globalQos;

	private long idleEventInterval;

	private long lastReceive = System.currentTimeMillis();

	private boolean statefulRetryFatalWithNullMessageId = true;

	private ConditionalExceptionLogger exclusiveConsumerExceptionLogger = new DefaultExclusiveConsumerLogger();

	private boolean alwaysRequeueWithTxManagerRollback;

	private String lookupKeyQualifier = "";

	private boolean forceCloseChannel = true;

	private String errorHandlerLoggerName = getClass().getName();

	private BatchingStrategy batchingStrategy = new SimpleBatchingStrategy(0, 0, 0L);

	private MicrometerHolder micrometerHolder;

	private boolean micrometerEnabled = true;

	private boolean observationEnabled = false;

	private boolean isBatchListener;

	private long consumeDelay;

	private JavaLangErrorHandler javaLangErrorHandler = error -> System.exit(EXIT_99);

	private volatile List<Queue> queues = new CopyOnWriteArrayList<>();

	private volatile boolean lazyLoad;

	private boolean asyncReplies;

	private MessageAckListener messageAckListener = (success, deliveryTag, cause) -> { };

	private RabbitListenerObservationConvention observationConvention;

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
	public final void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
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
	@Override
	public void setQueueNames(String... queueName) {
		Assert.noNullElements(queueName, "Queue name(s) cannot be null");
		setQueues(Arrays.stream(queueName)
				.map(Queue::new)
				.toArray(Queue[]::new));
	}

	/**
	 * Set the name of the queue(s) to receive messages from.
	 * @param queues the desired queue(s) (can not be <code>null</code>)
	 */
	public final void setQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' cannot be null");
		Assert.noNullElements(queues, "'queues' cannot contain null elements");
		if (isRunning()) {
			for (Queue queue : queues) {
				Assert.isTrue(StringUtils.hasText(queue.getName()), "Cannot add broker-named queues dynamically");
			}
		}
		this.queues = new CopyOnWriteArrayList<>(queues);
	}

	/**
	 * @return the name of the queues to receive messages from.
	 */
	public String[] getQueueNames() {
		return queuesToNames().toArray(new String[0]);
	}

	protected Set<String> getQueueNamesAsSet() {
		return new HashSet<>(queuesToNames());
	}

	/**
	 * Returns a map of current queue names to the Queue object; allows the
	 * determination of a changed broker-named queue.
	 * @return the map.
	 * @since 2.1
	 */
	protected Map<String, Queue> getQueueNamesToQueues() {
		return this.queues.stream()
				.collect(Collectors.toMap(Queue::getActualName, q -> q));
	}

	private List<String> queuesToNames() {
		return this.queues.stream()
				.map(Queue::getActualName)
				.collect(Collectors.toList());
	}

	/**
	 * Add queue(s) to this container's list of queues.
	 * @param queueNames The queue(s) to add.
	 */
	public void addQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' cannot be null");
		Assert.noNullElements(queueNames, "'queueNames' cannot contain null elements");
		addQueues(Arrays.stream(queueNames)
				.map(Queue::new)
				.toArray(Queue[]::new));
	}

	/**
	 * Add queue(s) to this container's list of queues.
	 * @param queues The queue(s) to add.
	 */
	public void addQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' cannot be null");
		Assert.noNullElements(queues, "'queues' cannot contain null elements");
		if (isRunning()) {
			for (Queue queue : queues) {
				Assert.hasText(queue.getName(), "Cannot add broker-named queues dynamically");
			}
		}
		this.queues.addAll(Arrays.asList(queues));
	}

	/**
	 * Remove queue(s) from this container's list of queues.
	 * @param queueNames The queue(s) to remove.
	 * @return the boolean result of removal on the target {@code queueNames} List.
	 */
	public boolean removeQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' cannot be null");
		Assert.noNullElements(queueNames, "'queueNames' cannot contain null elements");
		if (this.queues.size() > 0) {
			Set<String> toRemove = new HashSet<>(Arrays.asList(queueNames));
			return this.queues.removeIf(
					q -> toRemove.contains(q.getActualName()));
		}
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
		return removeQueueNames(Arrays.stream(queues)
				.map(Queue::getActualName)
				.toArray(String[]::new));
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
	 * Set the {@link MessageListener}.
	 * @param messageListener the listener.
	 * @since 2.0
	 */
	public void setMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		this.isBatchListener = messageListener instanceof BatchMessageListener
				|| messageListener instanceof ChannelAwareBatchMessageListener;
		this.asyncReplies = messageListener.isAsyncReplies();
	}

	/**
	 * Check the given message listener, throwing an exception if it does not correspond to a supported listener type.
	 * <p>
	 * Only a Spring {@link MessageListener} object will be accepted.
	 * @param listener the message listener object to check
	 * @throws IllegalArgumentException if the supplied listener is not a MessageListener
	 * @see MessageListener
	 */
	protected void checkMessageListener(Object listener) {
		if (!(listener instanceof MessageListener)) {
			throw new IllegalArgumentException("Message listener needs to be of type ["
					+ MessageListener.class.getName() + "] or [" + ChannelAwareMessageListener.class.getName() + "]");
		}
	}

	@Override
	@Nullable
	/**
	 * Get a reference to the message listener.
	 * @return the message listener.
	 */
	public MessageListener getMessageListener() {
		return this.messageListener;
	}

	/**
	 * Set an ErrorHandler to be invoked in case of any uncaught exceptions thrown while processing a Message. By
	 * default a {@link ConditionalRejectingErrorHandler} with its default list of fatal exceptions will be used.
	 *
	 * @param errorHandler The error handler.
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * Determine whether or not the container should de-batch batched
	 * messages (true) or call the listener with the batch (false). Default: true.
	 * @param deBatchingEnabled the deBatchingEnabled to set.
	 * @see #setBatchingStrategy(BatchingStrategy)
	 */
	public void setDeBatchingEnabled(boolean deBatchingEnabled) {
		this.deBatchingEnabled = deBatchingEnabled;
	}

	protected boolean isDeBatchingEnabled() {
		return this.deBatchingEnabled;
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
		Assert.notNull(adviceChain, "'adviceChain' cannot be null");
		this.adviceChain = Arrays.copyOf(adviceChain, adviceChain.length);
	}

	protected Advice[] getAdviceChain() {
		return this.adviceChain; // NOSONAR direct access
	}

	/**
	 * Set {@link MessagePostProcessor}s that will be applied after message reception, before
	 * invoking the {@link MessageListener}. Often used to decompress data.  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * @param afterReceivePostProcessors the post processor.
	 * @since 1.4.2
	 * @see #addAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public void setAfterReceivePostProcessors(MessagePostProcessor... afterReceivePostProcessors) {
		Assert.notNull(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot be null");
		Assert.noNullElements(afterReceivePostProcessors, "'afterReceivePostProcessors' cannot have null elements");
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(Arrays.asList(afterReceivePostProcessors));
	}

	/**
	 * Add {@link MessagePostProcessor}s that will be applied after message reception, before
	 * invoking the {@link MessageListener}. Often used to decompress data.  Processors are invoked in order,
	 * depending on {@code PriorityOrder}, {@code Order} and finally unordered.
	 * <p>
	 * In contrast to {@link #setAfterReceivePostProcessors(MessagePostProcessor...)}, this
	 * method does not override the previously added afterReceivePostProcessors.
	 * @param postprocessors the post processor.
	 * @since 2.1.4
	 */
	public void addAfterReceivePostProcessors(MessagePostProcessor... postprocessors) {
		Assert.notNull(postprocessors, "'afterReceivePostProcessors' cannot be null");
		if (this.afterReceivePostProcessors == null) {
			this.afterReceivePostProcessors = new ArrayList<>();
		}
		this.afterReceivePostProcessors.addAll(Arrays.asList(postprocessors));
		this.afterReceivePostProcessors = MessagePostProcessorUtils.sort(this.afterReceivePostProcessors);
	}

	/**
	 * Remove the provided {@link MessagePostProcessor} from the {@link #afterReceivePostProcessors} list.
	 * @param afterReceivePostProcessor the MessagePostProcessor to remove.
	 * @return the boolean if the provided post processor has been removed.
	 * @since 2.1.4
	 * @see #addAfterReceivePostProcessors(MessagePostProcessor...)
	 */
	public boolean removeAfterReceivePostProcessor(MessagePostProcessor afterReceivePostProcessor) {
		Assert.notNull(afterReceivePostProcessor, "'afterReceivePostProcessor' cannot be null");
		if (this.afterReceivePostProcessors != null) {
			return this.afterReceivePostProcessors.remove(afterReceivePostProcessor);
		}
		return false;
	}

	/**
	 * Set whether to automatically start the container after initialization.
	 * <p>
	 * Default is "true"; set this to "false" to allow for manual startup through the {@link #start()} method.
	 *
	 * @param autoStartup true for auto startup.
	 */
	@Override
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
	@Nullable
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
					.getTargetConnectionFactory(getRoutingLookupKey()); // NOSONAR never null
			if (targetConnectionFactory != null) {
				return targetConnectionFactory;
			}
		}
		return connectionFactory;
	}

	/**
	 * Set a qualifier that will prefix the connection factory lookup key; default none.
	 * @param lookupKeyQualifier the qualifier
	 * @since 1.6.9
	 * @see #getRoutingLookupKey()
	 */
	public void setLookupKeyQualifier(String lookupKeyQualifier) {
		this.lookupKeyQualifier = lookupKeyQualifier;
	}

	/**
	 * Force close the channel if the consumer threads don't respond to a shutdown.
	 * @return true to force close.
	 * @since 1.7.4
	 */
	protected boolean isForceCloseChannel() {
		return this.forceCloseChannel;
	}

	/**
	 * Set to true to force close the channel if the consumer threads don't respond to a
	 * shutdown. Default: true (since 2.0).
	 * @param forceCloseChannel true to force close.
	 * @since 1.7.4
	 */
	public void setForceCloseChannel(boolean forceCloseChannel) {
		this.forceCloseChannel = forceCloseChannel;
	}

	/**
	 * Return the lookup key if the connection factory is a
	 * {@link RoutingConnectionFactory}; null otherwise. The routing key is the
	 * comma-delimited list of queue names with all spaces removed and bracketed by [...],
	 * optionally prefixed by a qualifier, e.g. "foo[...]".
	 * @return the key or null.
	 * @since 1.6.9
	 * @see #setLookupKeyQualifier(String)
	 */
	@Nullable
	protected String getRoutingLookupKey() {
		return super.getConnectionFactory() instanceof RoutingConnectionFactory
				? this.lookupKeyQualifier + queuesAsListString()
				: null;
	}

	private String queuesAsListString() {
		return "[" + this.queues.stream()
				.map(Queue::getName)
				.collect(Collectors.joining(",")) + "]";
	}

	/**
	 * Return the (@link RoutingConnectionFactory} if the connection factory is a
	 * {@link RoutingConnectionFactory}; null otherwise.
	 * @return the {@link RoutingConnectionFactory} or null.
	 * @since 1.6.9
	 */
	@Nullable
	protected RoutingConnectionFactory getRoutingConnectionFactory() {
		return super.getConnectionFactory() instanceof RoutingConnectionFactory
				? (RoutingConnectionFactory) super.getConnectionFactory()
				: null;
	}

	/**
	 * The 'id' attribute of the listener.
	 * @return the id (or the container bean name if no id set).
	 */
	@Nullable
	public String getListenerId() {
		return this.listenerId != null ? this.listenerId : this.beanName;
	}

	@Override
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
	@Nullable
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
	public Map<String, Object> getConsumerArguments() {
		synchronized (this.consumersMonitor) {
			return new HashMap<>(this.consumerArgs);
		}
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
	 * Set to true for an no-local consumer.
	 * @param noLocal true for an no-local consumer.
	 */
	public void setNoLocal(boolean noLocal) {
		this.noLocal = noLocal;
	}

	/**
	 * Return whether the consumers should be no-local.
	 * @return true for no-local consumers.
	 */
	protected boolean isNoLocal() {
		return this.noLocal;
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
	 * @since 2.0
	 * @see #setDefaultRequeueRejected(boolean)
	 */
	protected boolean isDefaultRequeueRejected() {
		return this.defaultRequeueRejected;
	}

	/**
	 * Tell the broker how many messages to send to each consumer in a single request.
	 * Often this can be set quite high to improve throughput.
	 * @param prefetchCount the prefetch count
	 * @see com.rabbitmq.client.Channel#basicQos(int, boolean)
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
	 * Apply prefetchCount to the entire channel.
	 * @param globalQos true for a channel-wide prefetch.
	 * @since 2.2.17
	 * @see com.rabbitmq.client.Channel#basicQos(int, boolean)
	 */
	public void setGlobalQos(boolean globalQos) {
		this.globalQos = globalQos;
	}

	protected boolean isGlobalQos() {
		return this.globalQos;
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

	@Nullable
	protected PlatformTransactionManager getTransactionManager() {
		return this.transactionManager;
	}

	/**
	 * Set the transaction attribute to use when using an external transaction manager.
	 * @param transactionAttribute the transaction attribute to set
	 */
	public void setTransactionAttribute(TransactionAttribute transactionAttribute) {
		Assert.notNull(transactionAttribute, "'transactionAttribute' cannot be null");
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
		Assert.notNull(taskExecutor, "'taskExecutor' cannot be null");
		this.taskExecutor = taskExecutor;
		this.taskExecutorSet = true;
	}

	protected Executor getTaskExecutor() {
		return this.taskExecutor;
	}

	/**
	 * Specify the interval between recovery attempts, in <b>milliseconds</b>.
	 * The default is 5000 ms, that is, 5 seconds.
	 * @param recoveryInterval The recovery interval.
	 */
	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryBackOff = new FixedBackOff(recoveryInterval, FixedBackOff.UNLIMITED_ATTEMPTS);
	}

	/**
	 * Specify the {@link BackOff} for interval between recovery attempts.
	 * The default is 5000 ms, that is, 5 seconds.
	 * With the {@link BackOff} you can supply the {@code maxAttempts} for recovery before
	 * the {@link #stop()} will be performed.
	 * @param recoveryBackOff The BackOff to recover.
	 * @since 1.5
	 */
	public void setRecoveryBackOff(BackOff recoveryBackOff) {
		Assert.notNull(recoveryBackOff, "'recoveryBackOff' must not be null.");
		this.recoveryBackOff = recoveryBackOff;
	}

	protected BackOff getRecoveryBackOff() {
		return this.recoveryBackOff;
	}

	/**
	 * Set the {@link MessagePropertiesConverter} for this listener container.
	 * @param messagePropertiesConverter The properties converter.
	 */
	public void setMessagePropertiesConverter(MessagePropertiesConverter messagePropertiesConverter) {
		Assert.notNull(messagePropertiesConverter, "messagePropertiesConverter must not be null");
		this.messagePropertiesConverter = messagePropertiesConverter;
	}

	protected MessagePropertiesConverter getMessagePropertiesConverter() {
		return this.messagePropertiesConverter;
	}

	@Nullable
	protected AmqpAdmin getAmqpAdmin() {
		return this.amqpAdmin;
	}

	/**
	 * Set the {@link AmqpAdmin}, used to declare any auto-delete queues, bindings
	 * etc when the container is started. Only needed if those queues use conditional
	 * declaration (have a 'declared-by' attribute). If not specified, an internal
	 * admin will be used which will attempt to declare all elements not having a
	 * 'declared-by' attribute.
	 * @param amqpAdmin the AmqpAdmin to use
	 * @since 2.1
	 */
	public void setAmqpAdmin(AmqpAdmin amqpAdmin) {
		this.amqpAdmin = amqpAdmin;
	}

	/**
	 * If all of the configured queue(s) are not available on the broker, this setting
	 * determines whether the condition is fatal. When true, and
	 * the queues are missing during startup, the context refresh() will fail.
	 * <p> When false, the condition is not considered fatal and the container will
	 * continue to attempt to start the consumers.
	 * @param missingQueuesFatal the missingQueuesFatal to set.
	 * @since 1.3.5
	 * @see #setAutoDeclare(boolean)
	 */
	public void setMissingQueuesFatal(boolean missingQueuesFatal) {
		this.missingQueuesFatal = missingQueuesFatal;
		this.missingQueuesFatalSet = true;
	}

	protected boolean isMissingQueuesFatal() {
		return this.missingQueuesFatal;
	}

	protected boolean isMissingQueuesFatalSet() {
		return this.missingQueuesFatalSet;
	}

	/**
	 * Prevent the container from starting if any of the queues defined in the context have
	 * mismatched arguments (TTL etc). Default false.
	 * @param mismatchedQueuesFatal true to fail initialization when this condition occurs.
	 * @since 1.6
	 */
	public void setMismatchedQueuesFatal(boolean mismatchedQueuesFatal) {
		this.mismatchedQueuesFatal = mismatchedQueuesFatal;
	}

	protected boolean isMismatchedQueuesFatal() {
		return this.mismatchedQueuesFatal;
	}


	public void setPossibleAuthenticationFailureFatal(boolean possibleAuthenticationFailureFatal) {
		doSetPossibleAuthenticationFailureFatal(possibleAuthenticationFailureFatal);
		this.possibleAuthenticationFailureFatalSet = true;
	}

	protected final void doSetPossibleAuthenticationFailureFatal(boolean possibleAuthenticationFailureFatal) {
		this.possibleAuthenticationFailureFatal = possibleAuthenticationFailureFatal;
	}

	public boolean isPossibleAuthenticationFailureFatal() {
		return this.possibleAuthenticationFailureFatal;
	}

	protected boolean isPossibleAuthenticationFailureFatalSet() {
		return this.possibleAuthenticationFailureFatalSet;
	}
	protected boolean isAsyncReplies() {
		return this.asyncReplies;
	}

	/**
	 * Set to true to automatically declare elements (queues, exchanges, bindings)
	 * in the application context during container start().
	 * @param autoDeclare the boolean flag to indicate an declaration operation.
	 * @since 1.4
	 * @see #redeclareElementsIfNecessary
	 */
	public void setAutoDeclare(boolean autoDeclare) {
		this.autoDeclare = autoDeclare;
	}

	protected boolean isAutoDeclare() {
		return this.autoDeclare;
	}

	/**
	 * Set the interval between passive queue declaration attempts in milliseconds.
	 * @param failedDeclarationRetryInterval the interval, default 5000.
	 * @since 1.3.9
	 */
	public void setFailedDeclarationRetryInterval(long failedDeclarationRetryInterval) {
		this.failedDeclarationRetryInterval = failedDeclarationRetryInterval;
	}

	protected long getFailedDeclarationRetryInterval() {
		return this.failedDeclarationRetryInterval;
	}

	protected boolean isStatefulRetryFatalWithNullMessageId() {
		return this.statefulRetryFatalWithNullMessageId;
	}

	/**
	 * Set whether a message with a null messageId is fatal for the consumer
	 * when using stateful retry. When false, instead of stopping the consumer,
	 * the message is rejected and not requeued - it will be discarded or routed
	 * to the dead letter queue, if so configured. Default true.
	 * @param statefulRetryFatalWithNullMessageId true for fatal.
	 * @since 2.0
	 */
	public void setStatefulRetryFatalWithNullMessageId(boolean statefulRetryFatalWithNullMessageId) {
		this.statefulRetryFatalWithNullMessageId = statefulRetryFatalWithNullMessageId;
	}

	/**
	 * Set a {@link ConditionalExceptionLogger} for logging exclusive consumer failures. The
	 * default is to log such failures at WARN level.
	 * @param exclusiveConsumerExceptionLogger the conditional exception logger.
	 * @since 1.5
	 */
	public void setExclusiveConsumerExceptionLogger(ConditionalExceptionLogger exclusiveConsumerExceptionLogger) {
		this.exclusiveConsumerExceptionLogger = exclusiveConsumerExceptionLogger;
	}

	protected ConditionalExceptionLogger getExclusiveConsumerExceptionLogger() {
		return this.exclusiveConsumerExceptionLogger;
	}

	/**
	 * Set to true to always requeue on transaction rollback with an external
	 * {@link #setTransactionManager(PlatformTransactionManager) TransactionManager}.
	 * With earlier releases, when a transaction manager was configured, a transaction
	 * rollback always requeued the message. This was inconsistent with local transactions
	 * where the normal {@link #setDefaultRequeueRejected(boolean) defaultRequeueRejected}
	 * and {@link AmqpRejectAndDontRequeueException} logic was honored to determine whether
	 * the message was requeued. RabbitMQ does not consider the message delivery to be part
	 * of the transaction.
	 * This boolean was introduced in 1.7.1, set to true by default, to be consistent with
	 * previous behavior. Starting with version 2.0, it is false by default.
	 * @param alwaysRequeueWithTxManagerRollback true to always requeue on rollback.
	 * @since 1.7.1.
	 */
	public void setAlwaysRequeueWithTxManagerRollback(boolean alwaysRequeueWithTxManagerRollback) {
		this.alwaysRequeueWithTxManagerRollback = alwaysRequeueWithTxManagerRollback;
	}

	protected boolean isAlwaysRequeueWithTxManagerRollback() {
		return this.alwaysRequeueWithTxManagerRollback;
	}

	/**
	 * Set the name (category) of the logger used to log exceptions thrown by the error handler.
	 * It defaults to the container's logger but can be overridden if you want it to log at a different
	 * level to the container. Such exceptions are logged at the ERROR level.
	 * @param errorHandlerLoggerName the logger name.
	 * @since 2.0.8
	 */
	public void setErrorHandlerLoggerName(String errorHandlerLoggerName) {
		Assert.notNull(errorHandlerLoggerName, "'errorHandlerLoggerName' cannot be null");
		this.errorHandlerLoggerName = errorHandlerLoggerName;
	}

	/**
	 * Set a batching strategy to use when de-batching messages.
	 * Default is {@link SimpleBatchingStrategy}.
	 * @param batchingStrategy the strategy.
	 * @since 2.2
	 * @see #setDeBatchingEnabled(boolean)
	 */
	public void setBatchingStrategy(BatchingStrategy batchingStrategy) {
		Assert.notNull(batchingStrategy, "'batchingStrategy' cannot be null");
		this.batchingStrategy = batchingStrategy;
	}

	protected BatchingStrategy getBatchingStrategy() {
		return this.batchingStrategy;
	}

	protected Collection<MessagePostProcessor> getAfterReceivePostProcessors() {
		return this.afterReceivePostProcessors;
	}

	/**
	 * Set additional tags for the Micrometer listener timers.
	 * @param tags the tags.
	 * @since 2.2
	 */
	public void setMicrometerTags(Map<String, String> tags) {
		if (tags != null) {
			this.micrometerTags.putAll(tags);
		}
	}

	/**
	 * Set to false to disable micrometer listener timers.
	 * @param micrometerEnabled false to disable.
	 * @since 2.2
	 */
	public void setMicrometerEnabled(boolean micrometerEnabled) {
		this.micrometerEnabled = micrometerEnabled;
	}

	/**
	 * Enable observation via micrometer.
	 * @param observationEnabled true to enable.
	 * @since 3.0
	 */
	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}

	/**
	 * Set an observation convention; used to add additional key/values to observations.
	 * @param observationConvention the convention.
	 * @since 3.0
	 */
	public void setObservationConvention(RabbitListenerObservationConvention observationConvention) {
		this.observationConvention = observationConvention;
	}

	/**
	 * Get the consumeDelay - a time to wait before consuming in ms.
	 * @return the consume delay.
	 * @since 2.3
	 */
	protected long getConsumeDelay() {
		return this.consumeDelay;
	}

	/**
	 * Set the consumeDelay - a time to wait before consuming in ms. This is useful when
	 * using the sharding plugin with {@code concurrency > 1}, to avoid uneven distribution of
	 * consumers across the shards. See the plugin README for more information.
	 * @param consumeDelay the consume delay.
	 * @since 2.3
	 */
	public void setConsumeDelay(long consumeDelay) {
		this.consumeDelay = consumeDelay;
	}

	protected JavaLangErrorHandler getJavaLangErrorHandler() {
		return this.javaLangErrorHandler;
	}

	/**
	 * Provide a JavaLangErrorHandler implementation; by default, {@code System.exit(99)}
	 * is called.
	 * @param javaLangErrorHandler the handler.
	 * @since 2.2.12
	 */
	public void setjavaLangErrorHandler(JavaLangErrorHandler javaLangErrorHandler) {
		Assert.notNull(javaLangErrorHandler, "'javaLangErrorHandler' cannot be null");
		this.javaLangErrorHandler = javaLangErrorHandler;
	}

	/**
	 * Set a {@link MessageAckListener} to use when ack a message(messages) in
	 * {@link AcknowledgeMode#AUTO} mode.
	 * @param messageAckListener the messageAckListener.
	 * @since 2.4.6
	 * @see MessageAckListener
	 * @see AcknowledgeMode
	 */
	public void setMessageAckListener(MessageAckListener messageAckListener) {
		Assert.notNull(messageAckListener, "'messageAckListener' cannot be null");
		this.messageAckListener = messageAckListener;
	}

	protected MessageAckListener getMessageAckListener() {
		return this.messageAckListener;
	}

	/**
	 * Delegates to {@link #validateConfiguration()} and {@link #initialize()}.
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.state(
				this.exposeListenerChannel || !getAcknowledgeMode().isManual(),
				"You cannot acknowledge messages manually if the channel is not exposed to the listener "
						+ "(please check your configuration and set exposeListenerChannel=true or " +
						"acknowledgeMode!=MANUAL)");
		Assert.state(
				!(getAcknowledgeMode().isAutoAck() && isChannelTransacted()),
				"The acknowledgeMode is NONE (autoack in Rabbit terms) which is not consistent with having a "
						+ "transactional channel. Either use a different AcknowledgeMode or make sure " +
						"channelTransacted=false");
		validateConfiguration();
		initialize();
		try {
			if (this.micrometerHolder == null && MICROMETER_PRESENT && this.micrometerEnabled && !this.observationEnabled
					&& this.applicationContext != null) {
				String id = getListenerId();
				if (id == null) {
					id = "no_id_or_beanName";
				}
				this.micrometerHolder = new MicrometerHolder(this.applicationContext, id,
						this.micrometerTags);
			}
		}
		catch (IllegalStateException e) {
			this.logger.debug("Could not enable micrometer timers", e);
		}
		if (this.isAsyncReplies() && !AcknowledgeMode.MANUAL.equals(this.acknowledgeMode)) {
			this.acknowledgeMode = AcknowledgeMode.MANUAL;
		}
	}

	@Override
	public void setupMessageListener(MessageListener messageListener) {
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
		if (getAdviceChain().length == 0) {
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
		if (this.micrometerHolder != null) {
			this.micrometerHolder.destroy();
		}
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
			checkMissingQueuesFatalFromProperty();
			checkPossibleAuthenticationFailureFatalFromProperty();
			doInitialize();
			if (!this.isExposeListenerChannel() && this.transactionManager != null) {
				logger.warn("exposeListenerChannel=false is ignored when using a TransactionManager");
			}
			if (!this.taskExecutorSet && StringUtils.hasText(getListenerId())) {
				this.taskExecutor = new SimpleAsyncTaskExecutor(getListenerId() + "-");
				this.taskExecutorSet = true;
			}
			if (this.transactionManager != null && !isChannelTransacted()) {
				logger.debug("The 'channelTransacted' is coerced to 'true', when 'transactionManager' is provided");
				setChannelTransacted(true);
			}
			if (this.messageListener != null) {
				this.messageListener.containerAckMode(this.acknowledgeMode);
			}
			this.initialized = true;
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
	}

	/**
	 * Stop the shared Connection, call {@link #doShutdown()}, and close this container.
	 */
	public void shutdown() {
		synchronized (this.lifecycleMonitor) {
			if (!isActive()) {
				logger.debug("Shutdown ignored - container is not active already");
				this.lifecycleMonitor.notifyAll();
				return;
			}
			this.active = false;
			this.lifecycleMonitor.notifyAll();
		}

		logger.debug("Shutting down Rabbit listener container");

		// Shut down the invokers.
		try {
			doShutdown();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			setNotRunning();
		}
	}

	protected void setNotRunning() {
		synchronized (this.lifecycleMonitor) {
			this.running = false;
			this.lifecycleMonitor.notifyAll();
		}
	}

	/**
	 * Register any invokers within this container.
	 * <p>
	 * Subclasses need to implement this method for their specific invoker management process.
	 */
	protected abstract void doInitialize();

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
		if (isRunning()) {
			return;
		}
		if (!this.initialized) {
			synchronized (this.lifecycleMonitor) {
				if (!this.initialized) {
					afterPropertiesSet();
				}
			}
		}
		obtainObservationRegistry(this.applicationContext);
		try {
			logger.debug("Starting Rabbit listener container.");
			configureAdminIfNeeded();
			checkMismatchedQueues();
			doStart();
		}
		catch (Exception ex) {
			throw convertRabbitAccessException(ex);
		}
		finally {
			this.lazyLoad = false;
		}
	}

	/**
	 * Start this container, and notify all invoker tasks.
	 */
	protected void doStart() {
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
			setNotRunning();
		}
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
			try {
				this.errorHandler.handleError(ex);
			}
			catch (Exception e) {
				LogFactory.getLog(this.errorHandlerLoggerName).error(
						"Execution of Rabbit message listener failed, and the error handler threw an exception", e);
				throw e;
			}
		}
		else {
			logger.warn("Execution of Rabbit message listener failed, and no ErrorHandler has been set.", ex);
		}
	}

	// -------------------------------------------------------------------------
	// Template methods for listener execution
	// -------------------------------------------------------------------------

	/**
	 * Execute the specified listener, committing or rolling back the transaction afterwards (if necessary).
	 * @param channel the Rabbit Channel to operate on
	 * @param data the received Rabbit Message
	 * @see #invokeListener
	 * @see #handleListenerException
	 */
	protected void executeListener(Channel channel, Object data) {
		Observation observation;
		ObservationRegistry registry = getObservationRegistry();
		if (!this.observationEnabled || data instanceof List || registry == null) {
			observation = Observation.NOOP;
		}
		else {
			observation = RabbitListenerObservation.LISTENER_OBSERVATION.observation(registry,
						new MessageReceiverContext((Message) data))
					.lowCardinalityKeyValue(RabbitListenerObservation.ListenerLowCardinalityTags.LISTENER_ID.asString(),
							getListenerId());
			if (this.observationConvention != null) {
				observation.observationConvention(this.observationConvention);
			}
		}
		observation.observe(() -> executeListenerAndHandleException(channel, data));
	}

	@SuppressWarnings(UNCHECKED)
	protected void executeListenerAndHandleException(Channel channel, Object data) {
		if (!isRunning()) {
			if (logger.isWarnEnabled()) {
				logger.warn(
						"Rejecting received message(s) because the listener container has been stopped: " + data);
			}
			throw new MessageRejectedWhileStoppingException();
		}
		Object sample = null;
		if (this.micrometerHolder != null) {
			sample = this.micrometerHolder.start();
		}
		try {
			doExecuteListener(channel, data);
			if (sample != null) {
				this.micrometerHolder.success(sample, data instanceof Message
						? ((Message) data).getMessageProperties().getConsumerQueue()
						: queuesAsListString());
			}
		}
		catch (RuntimeException ex) {
			if (sample != null) {
				this.micrometerHolder.failure(sample, data instanceof Message
						? ((Message) data).getMessageProperties().getConsumerQueue()
						: queuesAsListString(), ex.getClass().getSimpleName());
			}
			Message message;
			if (data instanceof Message) {
				message = (Message) data;
			}
			else {
				message = ((List<Message>) data).get(0);
			}
			checkStatefulRetry(ex, message);
			handleListenerException(ex);
			throw ex;
		}
	}

	private void checkStatefulRetry(RuntimeException ex, Message message) {
		if (message.getMessageProperties().isFinalRetryForMessageWithNoId()) {
			if (this.statefulRetryFatalWithNullMessageId) {
				throw new FatalListenerExecutionException(
						"Illegal null id in message. Failed to manage retry for message: " + message, ex);
			}
			else {
				throw new ListenerExecutionFailedException("Cannot retry message more than once without an ID",
						new AmqpRejectAndDontRequeueException("Not retryable; rejecting and not requeuing", ex),
						message);
			}
		}
	}

	private void doExecuteListener(Channel channel, Object data) {
		if (data instanceof Message) {
			Message message = (Message) data;
			if (this.afterReceivePostProcessors != null) {
				for (MessagePostProcessor processor : this.afterReceivePostProcessors) {
					message = processor.postProcessMessage(message);
					if (message == null) {
						throw new ImmediateAcknowledgeAmqpException(
								"Message Post Processor returned 'null', discarding message");
					}
				}
			}
			if (this.deBatchingEnabled && this.batchingStrategy.canDebatch(message.getMessageProperties())) {
				this.batchingStrategy.deBatch(message, fragment -> invokeListener(channel, fragment));
			}
			else {
				invokeListener(channel, message);
			}
		}
		else {
			invokeListener(channel, data);
		}
	}

	protected void invokeListener(Channel channel, Object data) {
		this.proxy.invokeListener(channel, data);
	}

	/**
	 * Invoke the specified listener: either as standard MessageListener or (preferably) as SessionAwareMessageListener.
	 * @param channel the Rabbit Channel to operate on
	 * @param data the received Rabbit Message or List of Message.
	 * @see #setMessageListener(MessageListener)
	 */
	protected void actualInvokeListener(Channel channel, Object data) {
		Object listener = getMessageListener();
		if (listener instanceof ChannelAwareMessageListener) {
			doInvokeListener((ChannelAwareMessageListener) listener, channel, data);
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
				doInvokeListener((MessageListener) listener, data);
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
	 * An exception thrown from the listener will be wrapped in a {@link ListenerExecutionFailedException}.
	 * @param listener the Spring ChannelAwareMessageListener to invoke
	 * @param channel the Rabbit Channel to operate on
	 * @param data the received Rabbit Message or List of Message.
	 * @see ChannelAwareMessageListener
	 * @see #setExposeListenerChannel(boolean)
	 */
	@SuppressWarnings(UNCHECKED)
	protected void doInvokeListener(ChannelAwareMessageListener listener, Channel channel, Object data) {

		Message message = null;
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
				if (data instanceof List) {
					listener.onMessageBatch((List<Message>) data, channelToUse);
				}
				else {
					message = (Message) data;
					listener.onMessage(message, channelToUse);
				}
			}
			catch (Exception e) {
				throw wrapToListenerExecutionFailedExceptionIfNeeded(e, data);
			}
		}
		finally {
			cleanUpAfterInvoke(resourceHolder, channelToUse, boundHere); // NOSONAR channel not null here
		}
	}

	private void cleanUpAfterInvoke(@Nullable RabbitResourceHolder resourceHolder, Channel channelToUse,
			boolean boundHere) {

		if (resourceHolder != null && boundHere) {
			// so the channel exposed (because exposeListenerChannel is false) will be closed
			resourceHolder.setSynchronizedWithTransaction(false);
		}
		ConnectionFactoryUtils.releaseResources(resourceHolder); // NOSONAR - null check in method
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

	/**
	 * Invoke the specified listener as Spring Rabbit MessageListener.
	 * <p>
	 * Default implementation performs a plain invocation of the <code>onMessage</code> method.
	 * <p>
	 * Exception thrown from listener will be wrapped to {@link ListenerExecutionFailedException}.
	 *
	 * @param listener the Rabbit MessageListener to invoke
	 * @param data the received Rabbit Message or List of Message.
	 *
	 * @see org.springframework.amqp.core.MessageListener#onMessage
	 */
	@SuppressWarnings(UNCHECKED)
	protected void doInvokeListener(MessageListener listener, Object data) {
		Message message = null;
		try {
			if (data instanceof List) {
				listener.onMessageBatch((List<Message>) data);
			}
			else {
				message = (Message) data;
				listener.onMessage(message);
			}
		}
		catch (Exception e) {
			throw wrapToListenerExecutionFailedExceptionIfNeeded(e, data);
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
	 * @param data The failed message.
	 * @return If 'e' is of type {@link ListenerExecutionFailedException} - return 'e' as it is, otherwise wrap it to
	 * {@link ListenerExecutionFailedException} and return.
	 */
	@SuppressWarnings(UNCHECKED)
	protected ListenerExecutionFailedException wrapToListenerExecutionFailedExceptionIfNeeded(Exception e,
			Object data) {

		if (!(e instanceof ListenerExecutionFailedException)) {
			// Wrap exception to ListenerExecutionFailedException.
			if (data instanceof List) {
				return new ListenerExecutionFailedException("Listener threw exception", e,
						((List<Message>) data).toArray(new Message[0]));
			}
			else {
				return new ListenerExecutionFailedException("Listener threw exception", e, (Message) data);
			}
		}
		return (ListenerExecutionFailedException) e;
	}

	protected void publishConsumerFailedEvent(String reason, boolean fatal, @Nullable Throwable t) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher
					.publishEvent(t == null ? new ListenerContainerConsumerTerminatedEvent(this, reason) :
							new ListenerContainerConsumerFailedEvent(this, reason, t, fatal));
		}
	}

	protected void publishMissingQueueEvent(String queue) {
		if (this.applicationEventPublisher != null) {
			this.applicationEventPublisher
					.publishEvent(new MissingQueueEvent(this, queue));
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

	protected void configureAdminIfNeeded() {
		if (this.amqpAdmin == null && this.applicationContext != null) {
			Map<String, AmqpAdmin> admins =
					BeanFactoryUtils.beansOfTypeIncludingAncestors(this.applicationContext, AmqpAdmin.class,
							false, false);
			if (admins.size() == 1) {
				this.amqpAdmin = admins.values().iterator().next();
			}
			else {
				if ((isAutoDeclare() || isMismatchedQueuesFatal()) && this.logger.isDebugEnabled()) {
					logger.debug("For 'autoDeclare' and 'mismatchedQueuesFatal' to work, there must be exactly one "
							+ "AmqpAdmin in the context or you must inject one into this container; found: "
							+ admins.size() + " for container " + toString());
				}
				if (isMismatchedQueuesFatal()) {
					throw new IllegalStateException("When 'mismatchedQueuesFatal' is 'true', there must be exactly "
							+ "one AmqpAdmin in the context or you must inject one into this container; found: "
							+ admins.size() + " for container " + toString());
				}
			}
		}
	}

	protected void checkMismatchedQueues() {
		if (this.mismatchedQueuesFatal && this.amqpAdmin != null) {
			try {
				this.amqpAdmin.initialize();
			}
			catch (AmqpConnectException e) {
				logger.info("Broker not available; cannot check queue declarations");
			}
			catch (AmqpIOException e) {
				if (RabbitUtils.isMismatchedQueueArgs(e)) {
					throw new FatalListenerStartupException("Mismatched queues", e);
				}
				else {
					logger.info("Failed to get connection during start(): " + e);
				}
			}
		}
		else {
			try {
				Connection connection = getConnectionFactory().createConnection(); // NOSONAR
				if (connection != null) {
					connection.close();
				}
			}
			catch (Exception e) {
				logger.info("Broker not available; cannot force queue declarations during start: " + e.getMessage());
			}
		}
	}

	@Override
	public void lazyLoad() {
		if (this.mismatchedQueuesFatal) {
			if (this.missingQueuesFatal) {
				logger.warn("'mismatchedQueuesFatal' and 'missingQueuesFatal' are ignored during the initial start(), "
						+ "for lazily loaded containers");
			}
			else {
				logger.warn("'mismatchedQueuesFatal' is ignored during the initial start(), "
						+ "for lazily loaded containers");
			}
		}
		else if (this.missingQueuesFatal) {
			logger.warn("'missingQueuesFatal' is ignored during the initial start(), "
					+ "for lazily loaded containers");
		}
		this.lazyLoad = true;
	}

	/**
	 * Use {@link AmqpAdmin#initialize()} to redeclare everything if necessary.
	 * Since auto deletion of a queue can cause upstream elements
	 * (bindings, exchanges) to be deleted too, everything needs to be redeclared if
	 * a queue is missing.
	 * Declaration is idempotent so, aside from some network chatter, there is no issue,
	 * and we only will do it if we detect our queue is gone.
	 * <p>
	 * In general it makes sense only for the 'auto-delete' or 'expired' queues,
	 * but with the server TTL policy we don't have ability to determine 'expiration'
	 * option for the queue.
	 * <p>
	 * Starting with version 1.6, if
	 * {@link #setMismatchedQueuesFatal(boolean) mismatchedQueuesFatal} is true,
	 * the declarations are always attempted during restart so the listener will
	 * fail with a fatal error if mismatches occur.
	 */
	protected synchronized void redeclareElementsIfNecessary() {
		AmqpAdmin admin = getAmqpAdmin();
		if (!this.lazyLoad && admin != null && isAutoDeclare()) {
			try {
				attemptDeclarations(admin);
			}
			catch (Exception e) {
				if (RabbitUtils.isMismatchedQueueArgs(e)) {
					throw new FatalListenerStartupException("Mismatched queues", e);
				}
				logger.error("Failed to check/redeclare auto-delete queue(s).", e);
			}
		}
	}

	private void attemptDeclarations(AmqpAdmin admin) {
		ApplicationContext context = this.getApplicationContext();
		if (context != null) {
			Set<String> queueNames = getQueueNamesAsSet();
			Map<String, Queue> queueBeans = context.getBeansOfType(Queue.class);
			for (Entry<String, Queue> entry : queueBeans.entrySet()) {
				Queue queue = entry.getValue();
				if (isMismatchedQueuesFatal() || (queueNames.contains(queue.getName()) &&
						admin.getQueueProperties(queue.getName()) == null)) {
					if (logger.isDebugEnabled()) {
						logger.debug("Redeclaring context exchanges, queues, bindings.");
					}
					admin.initialize();
					break;
				}
			}
		}
	}

	/**
	 * Traverse the cause chain and, if an {@link ImmediateAcknowledgeAmqpException}
	 * is found before an {@link AmqpRejectAndDontRequeueException}, return true.
	 * An {@link Error} will take precedence.
	 * @param ex the exception
	 * @return true if we should ack immediately.
	 * @since 1.6.6
	 */
	protected boolean causeChainHasImmediateAcknowledgeAmqpException(Throwable ex) {
		if (ex instanceof Error) {
			return false;
		}
		Throwable cause = ex.getCause();
		while (cause != null) {
			if (cause instanceof ImmediateAcknowledgeAmqpException) {
				return true;
			}
			else if (cause instanceof AmqpRejectAndDontRequeueException || cause instanceof Error) {
				return false;
			}
			cause = cause.getCause();
		}
		return false;
	}

	/**
	 * A null resource holder is rare, but possible if the transaction attribute caused no
	 * transaction to be started (e.g. {@code TransactionDefinition.PROPAGATION_NONE}). In
	 * that case the delivery tags will have been processed manually.
	 * @param resourceHolder the bound resource holder (if a transaction is active).
	 * @param exception the exception.
	 */
	protected void prepareHolderForRollback(RabbitResourceHolder resourceHolder, RuntimeException exception) {
		if (resourceHolder != null) {
			resourceHolder.setRequeueOnRollback(isAlwaysRequeueWithTxManagerRollback() ||
					ContainerUtils.shouldRequeue(isDefaultRequeueRejected(), exception, logger));
		}
	}

	private void checkMissingQueuesFatalFromProperty() {
		if (!isMissingQueuesFatalSet()) {
			try {
				ApplicationContext context = getApplicationContext();
				if (context != null) {
					Properties properties = context.getBean("spring.amqp.global.properties", Properties.class);
					String missingQueuesFatalProperty = properties.getProperty("mlc.missing.queues.fatal");

					if (!StringUtils.hasText(missingQueuesFatalProperty)) {
						missingQueuesFatalProperty = properties.getProperty("smlc.missing.queues.fatal");
					}

					if (StringUtils.hasText(missingQueuesFatalProperty)) {
						setMissingQueuesFatal(Boolean.parseBoolean(missingQueuesFatalProperty));
					}
				}
			}
			catch (BeansException be) {
				logger.debug("No global properties bean");
			}
		}
	}

	private void checkPossibleAuthenticationFailureFatalFromProperty() {
		if (!isPossibleAuthenticationFailureFatalSet()) {
			try {
				ApplicationContext context = getApplicationContext();
				if (context != null) {
					Properties properties = context.getBean("spring.amqp.global.properties", Properties.class);
					String possibleAuthenticationFailureFatalProperty =
							properties.getProperty("mlc.possible.authentication.failure.fatal");
					if (StringUtils.hasText(possibleAuthenticationFailureFatalProperty)) {
						setPossibleAuthenticationFailureFatal(
								Boolean.parseBoolean(possibleAuthenticationFailureFatalProperty));
					}
				}
			}
			catch (BeansException be) {
				logger.debug("No global properties bean");
			}
		}
	}

	@Nullable
	protected List<Message> debatch(Message message) {
		if (this.isBatchListener && isDeBatchingEnabled()
				&& getBatchingStrategy().canDebatch(message.getMessageProperties())) {
			final List<Message> messageList = new ArrayList<>();
			getBatchingStrategy().deBatch(message, fragment -> messageList.add(fragment));
			return messageList;
		}
		return null;
	}

	@FunctionalInterface
	private interface ContainerDelegate {

		void invokeListener(Channel channel, Object data);

	}

	/**
	 * A handler for {@link Error} on the container thread(s).
	 * @since 2.2.12
	 *
	 */
	@FunctionalInterface
	public interface JavaLangErrorHandler {

		/**
		 * Handle the error; typically, the JVM will be terminated.
		 * @param error the error.
		 */
		void handle(Error error);

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

	/**
	 * A runtime exception to wrap a {@link Throwable}.
	 */
	@SuppressWarnings("serial")
	protected static final class WrappedTransactionException extends RuntimeException {

		protected WrappedTransactionException(Throwable cause) {
			super(cause);
		}

	}

	/**
	 * Default implementation of {@link ConditionalExceptionLogger} for logging exclusive
	 * consumer failures.
	 * @since 1.5
	 */
	private static class DefaultExclusiveConsumerLogger implements ConditionalExceptionLogger {

		DefaultExclusiveConsumerLogger() {
		}

		@Override
		public void log(Log logger, String message, Throwable t) {
			if (t instanceof ShutdownSignalException) {
				ShutdownSignalException cause = (ShutdownSignalException) t;
				if (RabbitUtils.isExclusiveUseChannelClose(cause)) {
					if (logger.isWarnEnabled()) {
						logger.warn(message + ": " + cause.toString());
					}
				}
				else if (!RabbitUtils.isNormalChannelClose(cause)) {
					logger.error(message + ": " + cause.getMessage());
				}
			}
			else {
				if (logger.isErrorEnabled()) {
					logger.error("Unexpected invocation of " + getClass() + ", with message: " + message, t);
				}
			}
		}

	}

}
