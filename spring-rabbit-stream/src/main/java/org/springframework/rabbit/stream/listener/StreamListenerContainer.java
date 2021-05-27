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

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.lang.Nullable;
import org.springframework.rabbit.stream.support.converter.StreamMessageConverter;
import org.springframework.util.Assert;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;

/**
 * A listener container for RabbitMQ Streams.
 *
 * @author Gary Russell
 * @since 2.4
 *
 */
public class StreamListenerContainer implements MessageListenerContainer, BeanNameAware {

	private final Environment environment;

	private final ConsumerBuilder builder;

	private final StreamMessageConverter messageConverter = new StreamMessageConverter();

	private java.util.function.Consumer<ConsumerBuilder> consumerCustomizer = c -> { };

	private String stream;

	private Consumer consumer;

	private String listenerId;

	private String beanName;

	/**
	 * Construct an instance using the provided environment.
	 * @param environment the environment.
	 */
	public StreamListenerContainer(Environment environment) {
		this.environment = environment;
		this.builder = environment.consumerBuilder()
				.offset(OffsetSpecification.first());
	}

	@Override
	public void setQueueNames(String... queueNames) {
		Assert.isTrue(queueNames != null && queueNames.length == 1, "Only one stream is supported");
		this.stream = queueNames[0];
		this.builder.stream(this.stream);
	}

	/**
	 * Customize the consumer builder before it is built.
	 * @param consumerCustomizer the customizer.
	 */
	public void setConsumerCustomizer(java.util.function.Consumer<ConsumerBuilder> consumerCustomizer) {
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
	public synchronized boolean isRunning() {
		return this.consumer != null;
	}

	@Override
	public synchronized void start() {
		if (this.consumer == null) {
			this.environment.streamCreator().stream(this.stream).create();
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
					catch (Exception e) {
						e.printStackTrace();
					}
				}
				else {
					messageListener.onMessage(message2);
				}
			}
		});
	}

}
