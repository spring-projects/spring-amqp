/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.rabbit.stream.listener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.aopalliance.aop.Advice;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.DefaultStreamMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;

/**
 * A listener container for RabbitMQ Streams.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamListenerContainer implements MessageListenerContainer, BeanNameAware {

	protected LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final ConsumerBuilder builder;

	private final Collection<Consumer> consumers = new ArrayList<>();

	private StreamMessageConverter streamConverter;

	private ConsumerCustomizer consumerCustomizer = (id, con) -> { };

	private boolean simpleStream;

	private boolean superStream;

	private int concurrency = 1;

	private String listenerId;

	private String beanName;

	private boolean autoStartup = true;

	private MessageListener messageListener;

	private StreamMessageListener streamListener;

	private Advice[] adviceChain;

	/**
	 * Construct an instance using the provided environment.
	 * @param environment the environment.
	 */
	public StreamListenerContainer(Environment environment) {
		this(environment, null);
	}

	/**
	 * Construct an instance using the provided environment and codec.
	 * @param environment the environment.
	 * @param codec the codec used to create reply messages.
	 */
	public StreamListenerContainer(Environment environment, @Nullable Codec codec) {
		Assert.notNull(environment, "'environment' cannot be null");
		this.builder = environment.consumerBuilder();
		this.streamConverter = new DefaultStreamMessageConverter(codec);
	}

	/**
	 * {@inheritDoc}
	 * Mutually exclusive with {@link #superStream(String, String)}.
	 */
	@Override
	public void setQueueNames(String... queueNames) {
		Assert.isTrue(!this.superStream, "setQueueNames() and superStream() are mutually exclusive");
		Assert.isTrue(queueNames != null && queueNames.length == 1, "Only one stream is supported");
		this.builder.stream(queueNames[0]);
		this.simpleStream = true;
	}

	/**
	 * Enable Single Active Consumer on a Super Stream, with one consumer.
	 * Mutually exclusive with {@link #setQueueNames(String...)}.
	 * @param streamName the stream.
	 * @param name the consumer name.
	 * @since 3.0
	 */
	public void superStream(String streamName, String name) {
		superStream(streamName, name, 1);
	}

	/**
	 * Enable Single Active Consumer on a Super Stream with the provided number of consumers.
	 * There must be at least that number of partitions in the Super Stream.
	 * Mutually exclusive with {@link #setQueueNames(String...)}.
	 * @param streamName the stream.
	 * @param name the consumer name.
	 * @param consumers the number of consumers.
	 * @since 3.0
	 */
	public synchronized void superStream(String streamName, String name, int consumers) {
		Assert.isTrue(consumers > 0, () -> "'concurrency' must be greater than zero, not " + consumers);
		this.concurrency = consumers;
		Assert.isTrue(!this.simpleStream, "setQueueNames() and superStream() are mutually exclusive");
		Assert.notNull(streamName, "'superStream' cannot be null");
		this.builder.superStream(streamName)
				.singleActiveConsumer()
				.name(name);
		this.superStream = true;
	}

	/**
	 * Get a {@link StreamMessageConverter} used to convert a
	 * {@link com.rabbitmq.stream.Message} to a
	 * {@link org.springframework.amqp.core.Message}.
	 * @return the converter.
	 */
	public StreamMessageConverter getStreamConverter() {
		return this.streamConverter;
	}

	/**
	 * Set a {@link StreamMessageConverter} used to convert a
	 * {@link com.rabbitmq.stream.Message} to a
	 * {@link org.springframework.amqp.core.Message}.
	 * @param messageConverter the converter.
	 */
	public void setStreamConverter(StreamMessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.streamConverter = messageConverter;
	}

	/**
	 * Customize the consumer builder before it is built.
	 * @param consumerCustomizer the customizer.
	 */
	public synchronized void setConsumerCustomizer(ConsumerCustomizer consumerCustomizer) {
		Assert.notNull(consumerCustomizer, "'consumerCustomizer' cannot be null");
		this.consumerCustomizer = consumerCustomizer;
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
	 * Return the bean name.
	 * @return the bean name.
	 */
	@Nullable
	public String getBeanName() {
		return this.beanName;
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	@Override
	public void setAutoStartup(boolean autoStart) {
		this.autoStartup = autoStart;
	}

	@Override
	public boolean isAutoStartup() {
		return this.autoStartup;
	}

	/**
	 * Set an advice chain to apply to the listener.
	 * @param advices the advice chain.
	 * @since 2.4.5
	 */
	public void setAdviceChain(Advice... advices) {
		Assert.notNull(advices, "'advices' cannot be null");
		Assert.noNullElements(advices, "'advices' cannot have null elements");
		this.adviceChain = Arrays.copyOf(advices, advices.length);
	}

	@Override
	@Nullable
	public Object getMessageListener() {
		return this.messageListener;
	}

	@Override
	public synchronized boolean isRunning() {
		return this.consumers.size() > 0;
	}

	@Override
	public synchronized void start() {
		if (this.consumers.size() == 0) {
			this.consumerCustomizer.accept(getListenerId(), this.builder);
			if (this.simpleStream) {
				this.consumers.add(this.builder.build());
			}
			else {
				for (int i = 0; i < this.concurrency; i++) {
					this.consumers.add(this.builder.build());
				}
			}
		}
	}

	@Override
	public synchronized void stop() {
		this.consumers.forEach(consumer -> {
			try {
				consumer.close();
			}
			catch (RuntimeException ex) {
				this.logger.error(ex, "Failed to close consumer");
			}
		});
		this.consumers.clear();
	}

	@Override
	public void setupMessageListener(MessageListener messageListener) {
		adviseIfNeeded(messageListener);
		this.builder.messageHandler((context, message) -> {
			if (this.streamListener != null) {
				this.streamListener.onStreamMessage(message, context);
			}
			else {
				Message message2 = this.streamConverter.toMessage(message, new StreamMessageProperties(context));
				if (this.messageListener instanceof ChannelAwareMessageListener) {
					try {
						((ChannelAwareMessageListener) this.messageListener).onMessage(message2, null);
					}
					catch (Exception ex) { // NOSONAR
						this.logger.error(ex, "Listner threw an exception");
					}
				}
				else {
					this.messageListener.onMessage(message2);
				}
			}
		});
	}

	private void adviseIfNeeded(MessageListener messageListener) {
		this.messageListener = messageListener;
		if (messageListener instanceof StreamMessageListener) {
			this.streamListener = (StreamMessageListener) messageListener;
		}
		if (this.adviceChain != null && this.adviceChain.length > 0) {
			ProxyFactory factory = new ProxyFactory(messageListener);
			for (Advice advice : this.adviceChain) {
				factory.addAdvisor(new DefaultPointcutAdvisor(advice));
			}
			factory.setInterfaces(messageListener.getClass().getInterfaces());
			if (this.streamListener != null) {
				this.streamListener = (StreamMessageListener) factory.getProxy(getClass().getClassLoader());
			}
			else {
				this.messageListener = (MessageListener) factory.getProxy(getClass().getClassLoader());
			}
		}
	}

}
