/*
 * Copyright 2021-2023 the original author or authors.
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
import org.springframework.amqp.rabbit.listener.MicrometerHolder;
import org.springframework.amqp.rabbit.listener.ObservableListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.support.RabbitExceptionTranslator;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.DefaultPointcutAdvisor;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.micrometer.RabbitStreamListenerObservation;
import org.springframework.rabbit.stream.micrometer.RabbitStreamListenerObservation.DefaultRabbitStreamListenerObservationConvention;
import org.springframework.rabbit.stream.micrometer.RabbitStreamListenerObservationConvention;
import org.springframework.rabbit.stream.micrometer.RabbitStreamMessageReceiverContext;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.DefaultStreamMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

/**
 * A listener container for RabbitMQ Streams.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamListenerContainer extends ObservableListenerContainer {

	protected LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR

	private final ConsumerBuilder builder;

	private final Collection<Consumer> consumers = new ArrayList<>();

	private StreamMessageConverter streamConverter;

	private ConsumerCustomizer consumerCustomizer = (id, con) -> { };

	private boolean simpleStream;

	private boolean superStream;

	private int concurrency = 1;

	private boolean autoStartup = true;

	private MessageListener messageListener;

	private StreamMessageListener streamListener;

	private Advice[] adviceChain;

	private String streamName;

	@Nullable
	private RabbitStreamListenerObservationConvention observationConvention;

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
	public synchronized void setQueueNames(String... queueNames) {
		Assert.isTrue(!this.superStream, "setQueueNames() and superStream() are mutually exclusive");
		Assert.isTrue(queueNames != null && queueNames.length == 1, "Only one stream is supported");
		this.builder.stream(queueNames[0]);
		this.simpleStream = true;
		this.streamName = queueNames[0];
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
		this.streamName = streamName;
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

	/**
	 * Set a RabbitStreamListenerObservationConvention; used to add additional key/values
	 * to observations when using a {@link StreamMessageListener}.
	 * @param observationConvention the convention.
	 * @since 3.0.5
	 */
	public void setObservationConvention(RabbitStreamListenerObservationConvention observationConvention) {
		this.observationConvention = observationConvention;
	}

	@Override
	public void afterPropertiesSet() {
		checkMicrometer();
		checkObservation();
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
			ObservationRegistry registry = getObservationRegistry();
			Object sample = null;
			MicrometerHolder micrometerHolder = getMicrometerHolder();
			if (micrometerHolder != null) {
				sample = micrometerHolder.start();
			}
			Observation observation =
					RabbitStreamListenerObservation.STREAM_LISTENER_OBSERVATION.observation(this.observationConvention,
							DefaultRabbitStreamListenerObservationConvention.INSTANCE,
							() -> new RabbitStreamMessageReceiverContext(message, getListenerId(), this.streamName),
							registry);
			Object finalSample = sample;
			if (this.streamListener != null) {
				observation.observe(() -> {
					try {
						this.streamListener.onStreamMessage(message, context);
						if (finalSample != null) {
							micrometerHolder.success(finalSample, this.streamName);
						}
					}
					catch (RuntimeException rtex) {
						if (finalSample != null) {
							micrometerHolder.failure(finalSample, this.streamName, rtex.getClass().getSimpleName());
						}
						throw rtex;
					}
					catch (Exception ex) {
						if (finalSample != null) {
							micrometerHolder.failure(finalSample, this.streamName, ex.getClass().getSimpleName());
						}
						throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
					}
				});
			}
			else {
				Message message2 = this.streamConverter.toMessage(message, new StreamMessageProperties(context));
				if (this.messageListener instanceof ChannelAwareMessageListener) {
					try {
						observation.observe(() -> {
							try {
								((ChannelAwareMessageListener) this.messageListener).onMessage(message2, null);
								if (finalSample != null) {
									micrometerHolder.success(finalSample, this.streamName);
								}
							}
							catch (RuntimeException rtex) {
								if (finalSample != null) {
									micrometerHolder.failure(finalSample, this.streamName,
											rtex.getClass().getSimpleName());
								}
								throw rtex;
							}
							catch (Exception ex) {
								if (finalSample != null) {
									micrometerHolder.failure(finalSample, this.streamName, ex.getClass().getSimpleName());
								}
								throw RabbitExceptionTranslator.convertRabbitAccessException(ex);
							}
						});
					}
					catch (Exception ex) { // NOSONAR
						this.logger.error(ex, "Listener threw an exception");
					}
				}
				else {
					observation.observe(() -> this.messageListener.onMessage(message2));
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
