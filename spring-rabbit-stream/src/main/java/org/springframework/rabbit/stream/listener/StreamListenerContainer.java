/*
 * Copyright 2021 the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.support.converter.DefaultStreamMessageConverter;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

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

	protected Log logger = LogFactory.getLog(getClass());

	private final Environment environment;

	private final ConsumerBuilder builder;

	private StreamMessageConverter messageConverter;

	private java.util.function.Consumer<ConsumerBuilder> consumerCustomizer = c -> { };

	private String stream;

	private Consumer consumer;

	private String listenerId;

	private String beanName;

	private boolean autoStartup = true;

	private MessageListener messageListener;

	/**
	 * Construct an instance using the provided environment.
	 * @param environment the environment.
	 */
	public StreamListenerContainer(Environment environment) {
		Assert.notNull(environment, "'environment' cannot be null");
		this.environment = environment;
		this.builder = environment.consumerBuilder();
		this.messageConverter = new DefaultStreamMessageConverter(environment);
	}

	@Override
	public void setQueueNames(String... queueNames) {
		Assert.isTrue(queueNames != null && queueNames.length == 1, "Only one stream is supported");
		this.stream = queueNames[0];
		this.builder.stream(this.stream);
	}

	/**
	 * Get a {@link StreamMessageConverter} used to convert a
	 * {@link com.rabbitmq.stream.Message} to a
	 * {@link org.springframework.amqp.core.Message}.
	 * @return the converter.
	 */
	public StreamMessageConverter getMessageConverter() {
		return this.messageConverter;
	}

	/**
	 * Set a {@link StreamMessageConverter} used to convert a
	 * {@link com.rabbitmq.stream.Message} to a
	 * {@link org.springframework.amqp.core.Message}.
	 * @param messageConverter the converter.
	 */
	public void setMessageConverter(StreamMessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * Customize the consumer builder before it is built.
	 * @param consumerCustomizer the customizer.
	 */
	public synchronized void setConsumerCustomizer(java.util.function.Consumer<ConsumerBuilder> consumerCustomizer) {
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
	@Override
	@Nullable
	public Object getMessageListener() {
		return this.messageListener;
	}

	@Override
	public synchronized boolean isRunning() {
		return this.consumer != null;
	}

	@Override
	public synchronized void start() {
		if (this.consumer == null) {
			this.consumerCustomizer.accept(this.builder);
			this.consumer = this.builder.build();
		}
	}

	@Override
	public synchronized void stop() {
		if (this.consumer != null) {
			this.consumer.close();
			this.consumer = null;
		}
	}

	@Override
	public void setupMessageListener(MessageListener messageListener) {
		this.messageListener = messageListener;
		this.builder.messageHandler((context, message) -> {
			if (messageListener instanceof StreamMessageListener) {
				((StreamMessageListener) messageListener).onStreamMessage(message, context);
			}
			else {
				Message message2 = this.messageConverter.toMessage(message, new StreamMessageProperties(context));
				if (messageListener instanceof ChannelAwareMessageListener) {
					try {
						((ChannelAwareMessageListener) messageListener).onMessage(message2, null);
					}
					catch (Exception e) { // NOSONAR
						this.logger.error("Listner threw an exception", e);
					}
				}
				else {
					messageListener.onMessage(message2);
				}
			}
		});
	}

}
