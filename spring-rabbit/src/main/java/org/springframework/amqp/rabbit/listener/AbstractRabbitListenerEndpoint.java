/*
 * Copyright 2014-2024 the original author or authors.
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
import java.util.Map;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.batch.BatchingStrategy;
import org.springframework.amqp.rabbit.listener.adapter.ReplyPostProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.task.TaskExecutor;
import org.springframework.expression.BeanResolver;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base model for a Rabbit listener endpoint.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Ngoc Nhan
 *
 * @since 1.4
 *
 * @see MethodRabbitListenerEndpoint
 * @see org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint
 */
public abstract class AbstractRabbitListenerEndpoint implements RabbitListenerEndpoint, BeanFactoryAware {

	private String id;

	private final Collection<Queue> queues = new ArrayList<>();

	private final Collection<String> queueNames = new ArrayList<>();

	private boolean exclusive;

	private Integer priority;

	private String concurrency;

	private AmqpAdmin admin;

	private BeanFactory beanFactory;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private BeanResolver beanResolver;

	private String group;

	private Boolean autoStartup;

	private MessageConverter messageConverter;

	private TaskExecutor taskExecutor;

	private Boolean batchListener;

	private BatchingStrategy batchingStrategy;

	private AcknowledgeMode ackMode;

	private ReplyPostProcessor replyPostProcessor;

	private String replyContentType;

	private boolean converterWinsContentType = true;

	@Override
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		this.beanFactory = beanFactory;
		if (beanFactory instanceof ConfigurableListableBeanFactory clbf) {
			this.resolver = clbf.getBeanExpressionResolver();
			this.expressionContext = new BeanExpressionContext(clbf, null);
		}
		this.beanResolver = new BeanFactoryResolver(beanFactory);
	}

	@Nullable
	protected BeanFactory getBeanFactory() {
		return this.beanFactory;
	}

	protected BeanExpressionResolver getResolver() {
		return this.resolver;
	}

	protected BeanResolver getBeanResolver() {
		return this.beanResolver;
	}

	protected BeanExpressionContext getBeanExpressionContext() {
		return this.expressionContext;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	/**
	 * Set the queues to use. Either the {@link Queue} instances or the
	 * queue names should be provided, but not both.
	 * @param queues to set.
	 * @see #setQueueNames
	 */
	public void setQueues(Queue... queues) {
		Assert.notNull(queues, "'queues' must not be null");
		this.queues.clear();
		this.queues.addAll(Arrays.asList(queues));
	}

	/**
	 * @return the queues for this endpoint.
	 */
	public Collection<Queue> getQueues() {
		return this.queues;
	}

	/**
	 * @return the queue names for this endpoint.
	 */
	public Collection<String> getQueueNames() {
		return this.queueNames;
	}

	/**
	 * Set the queue names to use. Either the {@link Queue} instances or the
	 * queue names should be provided, but not both.
	 * @param queueNames to set.
	 * @see #setQueues
	 */
	public void setQueueNames(String... queueNames) {
		Assert.notNull(queueNames, "'queueNames' must not be null");
		this.queueNames.clear();
		this.queueNames.addAll(Arrays.asList(queueNames));
	}

	/**
	 * Set if a single consumer in the container will have exclusive use of the
	 * queues, preventing other consumers from receiving messages from the
	 * queue(s).
	 * @param exclusive the exclusive {@code boolean} flag.
	 */
	public void setExclusive(boolean exclusive) {
		this.exclusive = exclusive;
	}

	/**
	 * @return the exclusive {@code boolean} flag.
	 */
	public boolean isExclusive() {
		return this.exclusive;
	}

	/**
	 * Set the priority of this endpoint.
	 * @param priority the priority value.
	 */
	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	/**
	 * @return the priority of this endpoint or {@code null} if
	 * no priority is set.
	 */
	public Integer getPriority() {
		return this.priority;
	}

	/**
	 * Set the concurrency of this endpoint; usually overrides any concurrency
	 * settings on the container factory. Contents depend on container implementation.
	 * @param concurrency the concurrency.
	 * @since 2.0
	 */
	public void setConcurrency(String concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * The concurrency of this endpoint; Not used by this abstract class;
	 * used by subclasses to set the concurrency appropriate for the container type.
	 * @return the concurrency.
	 * @since 2.0
	 */
	@Override
	public String getConcurrency() {
		return this.concurrency;
	}

	/**
	 * Set the {@link AmqpAdmin} instance to use.
	 * @param admin the {@link AmqpAdmin} instance.
	 */
	public void setAdmin(AmqpAdmin admin) {
		this.admin = admin;
	}

	/**
	 * @return the {@link AmqpAdmin} instance to use or {@code null} if
	 * none is configured.
	 */
	public AmqpAdmin getAdmin() {
		return this.admin;
	}

	@Override
	public String getGroup() {
		return this.group;
	}

	/**
	 * Set the group for the corresponding listener container.
	 * @param group the group.
	 * @since 1.5
	 */
	public void setGroup(String group) {
		this.group = group;
	}


	/**
	 * Override the default autoStartup property.
	 * @param autoStartup the autoStartup.
	 * @since 2.0
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	@Override
	public Boolean getAutoStartup() {
		return this.autoStartup;
	}

	@Override
	public MessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	@Override
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public TaskExecutor getTaskExecutor() {
		return this.taskExecutor;
	}

	/**
	 * Override the default task executor.
	 * @param taskExecutor the executor.
	 * @since 2.2
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		this.taskExecutor = taskExecutor;
	}

	/**
	 * True if this endpoint is for a batch listener.
	 * @return true if batch.
	 */
	public boolean isBatchListener() {
		return this.batchListener != null && this.batchListener;
	}

	@Override
	/**
	 * True if this endpoint is for a batch listener.
	 * @return {@link Boolean#TRUE} if batch.
	 * @since 3.0
	 */
	@Nullable
	public Boolean getBatchListener() {
		return this.batchListener;
	}

	/**
	 * Set to true if this endpoint should create a batch listener.
	 * @param batchListener true for a batch listener.
	 * @since 2.2
	 * @see #setBatchingStrategy(BatchingStrategy)
	 */
	@Override
	public void setBatchListener(boolean batchListener) {
		this.batchListener = batchListener;
	}

	@Override
	@Nullable
	public BatchingStrategy getBatchingStrategy() {
		return this.batchingStrategy;
	}

	@Override
	public void setBatchingStrategy(BatchingStrategy batchingStrategy) {
		this.batchingStrategy = batchingStrategy;
	}

	@Override
	@Nullable
	public AcknowledgeMode getAckMode() {
		return this.ackMode;
	}

	public void setAckMode(AcknowledgeMode mode) {
		this.ackMode = mode;
	}

	@Override
	public ReplyPostProcessor getReplyPostProcessor() {
		return this.replyPostProcessor;
	}

	/**
	 * Set a {@link ReplyPostProcessor} to post process a response message before it is sent.
	 * @param replyPostProcessor the post processor.
	 * @since 2.2.5
	 */
	public void setReplyPostProcessor(ReplyPostProcessor replyPostProcessor) {
		this.replyPostProcessor = replyPostProcessor;
	}

	@Override
	public String getReplyContentType() {
		return this.replyContentType;
	}

	/**
	 * Set the reply content type.
	 * @param replyContentType the content type.
	 * @since 2.3
	 */
	public void setReplyContentType(String replyContentType) {
		this.replyContentType = replyContentType;
	}

	@Override
	public boolean isConverterWinsContentType() {
		return this.converterWinsContentType;
	}

	/**
	 * Set whether the content type set by a converter prevails or not.
	 * @param converterWinsContentType false to always apply the reply content type.
	 * @since 2.3
	 */
	public void setConverterWinsContentType(boolean converterWinsContentType) {
		this.converterWinsContentType = converterWinsContentType;
	}

	@Override
	public void setupListenerContainer(MessageListenerContainer listenerContainer) {
		Collection<String> qNames = getQueueNames();
		boolean queueNamesEmpty = qNames.isEmpty();
		if (listenerContainer instanceof AbstractMessageListenerContainer container) {
			boolean queuesEmpty = getQueues().isEmpty();
			if (!queuesEmpty && !queueNamesEmpty) {
				throw new IllegalStateException("Queues or queue names must be provided but not both for " + this);
			}
			if (queuesEmpty) {
				container.setQueueNames(qNames.toArray(new String[0]));
			}
			else {
				Collection<Queue> instances = getQueues();
				container.setQueues(instances.toArray(new Queue[0]));
			}

			container.setExclusive(isExclusive());
			if (getPriority() != null) {
				Map<String, Object> args = container.getConsumerArguments();
				args.put("x-priority", getPriority());
				container.setConsumerArguments(args);
			}

			if (getAdmin() != null) {
				container.setAmqpAdmin(getAdmin());
			}
		}
		else {
			Assert.state(!queueNamesEmpty, "At least one queue name is required");
			listenerContainer.setQueueNames(qNames.toArray(new String[0]));
		}
		setupMessageListener(listenerContainer);
	}

	/**
	 * Create a {@link MessageListener} that is able to serve this endpoint for the
	 * specified container.
	 * @param container the {@link MessageListenerContainer} to create a {@link MessageListener}.
	 * @return a {@link MessageListener} instance.
	 */
	protected abstract MessageListener createMessageListener(MessageListenerContainer container);

	private void setupMessageListener(MessageListenerContainer container) {
		MessageListener messageListener = createMessageListener(container);
		Assert.state(messageListener != null, () -> "Endpoint [" + this + "] must provide a non null message listener");
		container.setupMessageListener(messageListener);
	}

	/**
	 * @return a description for this endpoint.
	 * <p>Available to subclasses, for inclusion in their {@code toString()} result.
	 */
	protected StringBuilder getEndpointDescription() {
		StringBuilder result = new StringBuilder();
		return result.append(getClass().getSimpleName()).append("[").append(this.id).
				append("] queues=").append(this.queues).
				append("' | queueNames='").append(this.queueNames).
				append("' | exclusive='").append(this.exclusive).
				append("' | priority='").append(this.priority).
				append("' | admin='").append(this.admin).append("'");
	}

	@Override
	public String toString() {
		return getEndpointDescription().toString();
	}

}
