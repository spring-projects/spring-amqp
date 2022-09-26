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

package org.springframework.rabbit.stream.producer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.rabbit.stream.support.converter.DefaultStreamMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;

/**
 * Default implementation of {@link RabbitStreamOperations}.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class RabbitStreamTemplate implements RabbitStreamOperations, BeanNameAware {

	protected final LogAccessor logger = new LogAccessor(getClass()); // NOSONAR

	private final Environment environment;

	private final String streamName;

	private Function<com.rabbitmq.stream.Message, String> superStreamRouting;

	private MessageConverter messageConverter = new SimpleMessageConverter();

	private StreamMessageConverter streamConverter = new DefaultStreamMessageConverter();

	private boolean streamConverterSet;

	private Producer producer;

	private String beanName;

	private ProducerCustomizer producerCustomizer = (name, builder) -> { };

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


	private synchronized Producer createOrGetProducer() {
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
						() ->  this.producer.messageBuilder());
			}
		}
		return this.producer;
	}

	@Override
	public synchronized void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Add a routing function, making the stream a super stream.
	 * @param superStreamRouting the routing function.
	 * @since 3.0
	 */
	public void setSuperStreamRouting(Function<com.rabbitmq.stream.Message, String> superStreamRouting) {
		this.superStreamRouting = superStreamRouting;
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
	public synchronized void setStreamConverter(StreamMessageConverter streamConverter) {
		Assert.notNull(streamConverter, "'streamConverter' cannot be null");
		this.streamConverter = streamConverter;
		this.streamConverterSet = true;
	}

	/**
	 * Used to customize the {@link ProducerBuilder} before the {@link Producer} is built.
	 * @param producerCustomizer the customizer;
	 */
	public synchronized void setProducerCustomizer(ProducerCustomizer producerCustomizer) {
		Assert.notNull(producerCustomizer, "'producerCustomizer' cannot be null");
		this.producerCustomizer = producerCustomizer;
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
		createOrGetProducer().send(this.streamConverter.fromMessage(message), handleConfirm(future));
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
		createOrGetProducer().send(message, handleConfirm(future));
		return future;
	}

	@Override
	public MessageBuilder messageBuilder() {
		return createOrGetProducer().messageBuilder();
	}

	private ConfirmationHandler handleConfirm(CompletableFuture<Boolean> future) {
		return confStatus -> {
			if (confStatus.isConfirmed()) {
				future.complete(true);
			}
			else {
				int code = confStatus.getCode();
				String errorMessage;
				switch (code) {
				case Constants.CODE_MESSAGE_ENQUEUEING_FAILED:
					errorMessage = "Message Enqueueing Failed";
					break;
				case Constants.CODE_PRODUCER_CLOSED:
					errorMessage = "Producer Closed";
					break;
				case Constants.CODE_PRODUCER_NOT_AVAILABLE:
					errorMessage = "Producer Not Available";
					break;
				case Constants.CODE_PUBLISH_CONFIRM_TIMEOUT:
					errorMessage = "Publish Confirm Timeout";
					break;
				default:
					errorMessage = "Unknown code: " + code;
					break;
				}
				future.completeExceptionally(new StreamSendException(errorMessage, code));
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
	public synchronized void close() {
		if (this.producer != null) {
			this.producer.close();
			this.producer = null;
		}
	}

}
