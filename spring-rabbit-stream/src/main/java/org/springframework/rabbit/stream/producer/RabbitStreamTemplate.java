/*
 * Copyright 2021-2025 the original author or authors.
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

package org.springframework.rabbit.stream.producer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.micrometer.RabbitStreamMessageSenderContext;
import org.springframework.rabbit.stream.micrometer.RabbitStreamTemplateObservation;
import org.springframework.rabbit.stream.micrometer.RabbitStreamTemplateObservation.DefaultRabbitStreamTemplateObservationConvention;
import org.springframework.rabbit.stream.micrometer.RabbitStreamTemplateObservationConvention;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.DefaultStreamMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RabbitStreamOperations}.
 *
 * @author Gary Russell
 * @author Christian Tzolov
 * @author Ngoc Nhan
 * @since 2.4
 *
 */
public class RabbitStreamTemplate implements RabbitStreamOperations, ApplicationContextAware, BeanNameAware {

	protected final LogAccessor logger = new LogAccessor(getClass()); // NOSONAR

	private final Lock lock = new ReentrantLock();

	private ApplicationContext applicationContext;

	private final Environment environment;

	private final String streamName;

	private Function<com.rabbitmq.stream.Message, String> superStreamRouting;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private StreamMessageConverter streamConverter = new DefaultStreamMessageConverter();

	private boolean streamConverterSet;

	private String beanName;

	private ProducerCustomizer producerCustomizer = (name, builder) -> { };

	private boolean observationEnabled;

	@Nullable
	private RabbitStreamTemplateObservationConvention observationConvention;

	private ObservationRegistry observationRegistry;

	private volatile Producer producer;

	private volatile boolean observationRegistryObtained;

	/**
	 * Construct an instance with the provided {@link Environment}.
	 * @param environment the environment.
	 * @param streamName the stream name.
	 */
	public RabbitStreamTemplate(Environment environment, String streamName) {
		Assert.notNull(environment, "'environment' cannot be null");
		Assert.notNull(streamName, "'streamName' cannot be null");
		this.environment = environment;
		this.streamName = streamName;
	}


	private Producer createOrGetProducer() {
		if (this.producer == null) {
			this.lock.lock();
			try {
				if (this.producer == null) {
					ProducerBuilder builder = this.environment.producerBuilder();
					if (this.superStreamRouting == null) {
						builder.stream(this.streamName);
					}
					else {
						builder.superStream(this.streamName)
								.routing(this.superStreamRouting);
					}
					this.producerCustomizer.accept(this.beanName, builder);
					this.producer = builder.build();
					if (!this.streamConverterSet) {
						((DefaultStreamMessageConverter) this.streamConverter).setBuilderSupplier(
								() -> this.producer.messageBuilder());
					}
				}
			}
			finally {
				this.lock.unlock();
			}
		}
		return this.producer;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public void setBeanName(String name) {
		this.lock.lock();
		try {
			this.beanName = name;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Add a routing function, making the stream a super stream.
	 * @param superStreamRouting the routing function.
	 * @since 3.0
	 */
	public void setSuperStreamRouting(Function<com.rabbitmq.stream.Message, String> superStreamRouting) {
		this.lock.lock();
		try {
			this.superStreamRouting = superStreamRouting;
		}
		finally {
			this.lock.unlock();
		}
	}


	/**
	 * Set a converter for {@link #convertAndSend(Object)} operations.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		Assert.notNull(messageConverter, "'messageConverter' cannot be null");
		this.messageConverter = messageConverter;
	}

	/**
	 * Set a converter to convert from {@link Message} to {@link com.rabbitmq.stream.Message}
	 * for {@link #send(Message)} and {@link #convertAndSend(Object)} methods.
	 * @param streamConverter the converter.
	 */
	public void setStreamConverter(StreamMessageConverter streamConverter) {
		Assert.notNull(streamConverter, "'streamConverter' cannot be null");
		this.lock.lock();
		try {
			this.streamConverter = streamConverter;
			this.streamConverterSet = true;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Used to customize the {@link ProducerBuilder} before the {@link Producer} is built.
	 * @param producerCustomizer the customizer;
	 */
	public void setProducerCustomizer(ProducerCustomizer producerCustomizer) {
		Assert.notNull(producerCustomizer, "'producerCustomizer' cannot be null");
		this.lock.lock();
		try {
			this.producerCustomizer = producerCustomizer;
		}
		finally {
			this.lock.unlock();
		}
	}

	/**
	 * Set to true to enable Micrometer observation.
	 * @param observationEnabled true to enable.
	 * @since 3.0.5
	 */
	public void setObservationEnabled(boolean observationEnabled) {
		this.observationEnabled = observationEnabled;
	}


	@Override
	public MessageConverter messageConverter() {
		return this.messageConverter;
	}


	@Override
	public StreamMessageConverter streamMessageConverter() {
		return this.streamConverter;
	}


	@Override
	public CompletableFuture<Boolean> send(Message message) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		observeSend(this.streamConverter.fromMessage(message), future);
		return future;
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(Object message) {
		return convertAndSend(message, null);
	}

	@Override
	public CompletableFuture<Boolean> convertAndSend(Object message, @Nullable MessagePostProcessor mpp) {
		Message message2 = this.messageConverter.toMessage(message, new StreamMessageProperties());
		Assert.notNull(message2, "The message converter returned null");
		if (mpp != null) {
			message2 = mpp.postProcessMessage(message2);
			if (message2 == null) {
				this.logger.debug("Message Post Processor returned null, message not sent");
				CompletableFuture<Boolean> future = new CompletableFuture<>();
				future.complete(false);
				return future;
			}
		}
		return send(message2);
	}


	@Override
	public CompletableFuture<Boolean> send(com.rabbitmq.stream.Message message) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		observeSend(message, future);
		return future;
	}

	private void observeSend(com.rabbitmq.stream.Message message, CompletableFuture<Boolean> future) {
		Observation observation = RabbitStreamTemplateObservation.STREAM_TEMPLATE_OBSERVATION.observation(
				this.observationConvention, DefaultRabbitStreamTemplateObservationConvention.INSTANCE,
				() -> new RabbitStreamMessageSenderContext(message, this.beanName, this.streamName),
					obtainObservationRegistry());
		observation.start();
		try {
			createOrGetProducer().send(message, handleConfirm(future, observation));
		}
		catch (Exception ex) {
			observation.error(ex);
			observation.stop();
			future.completeExceptionally(ex);
		}
	}

	@Nullable
	private ObservationRegistry obtainObservationRegistry() {
		if (!this.observationRegistryObtained && this.observationEnabled) {
			if (this.applicationContext != null) {
				ObjectProvider<ObservationRegistry> registry =
						this.applicationContext.getBeanProvider(ObservationRegistry.class);
				this.observationRegistry = registry.getIfUnique();
			}
			this.observationRegistryObtained = true;
		}
		return this.observationRegistry;
	}

	@Override
	public MessageBuilder messageBuilder() {
		return createOrGetProducer().messageBuilder();
	}

	private ConfirmationHandler handleConfirm(CompletableFuture<Boolean> future, Observation observation) {
		return confStatus -> {
			if (confStatus.isConfirmed()) {
				future.complete(true);
				observation.stop();
			}
			else {
				int code = confStatus.getCode();
				String errorMessage = switch (code) {
					case Constants.CODE_MESSAGE_ENQUEUEING_FAILED -> "Message Enqueueing Failed";
					case Constants.CODE_PRODUCER_CLOSED -> "Producer Closed";
					case Constants.CODE_PRODUCER_NOT_AVAILABLE -> "Producer Not Available";
					case Constants.CODE_PUBLISH_CONFIRM_TIMEOUT -> "Publish Confirm Timeout";
					default -> "Unknown code: " + code;
				};
				StreamSendException ex = new StreamSendException(errorMessage, code);
				observation.error(ex);
				observation.stop();
				future.completeExceptionally(ex);
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * <b>Close the underlying producer; a new producer will be created on the next
	 * operation that requires one.</b>
	 */
	@Override
	public void close() {
		if (this.producer != null) {
			this.lock.lock();
			try {
				if (this.producer != null) {
					this.producer.close();
					this.producer = null;
				}
			}
			finally {
				this.lock.unlock();
			}
		}
	}

}
