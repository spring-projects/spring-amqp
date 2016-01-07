/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.amqp.rabbit.config;


import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpoint;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.util.ErrorHandler;

/**
 * Base {@link RabbitListenerContainerFactory} for Spring's base container implementation.
 *
 * @author Stephane Nicoll
 * @since 1.4
 * @see AbstractMessageListenerContainer
 */
public abstract class AbstractRabbitListenerContainerFactory<C extends AbstractMessageListenerContainer>
		implements RabbitListenerContainerFactory<C> {

	protected final Log logger = LogFactory.getLog(getClass());

	private ConnectionFactory connectionFactory;

	private ErrorHandler errorHandler;

	private MessageConverter messageConverter;

	private AcknowledgeMode acknowledgeMode;

	private Boolean channelTransacted;

	private Boolean autoStartup;

	private Integer phase;

	protected final AtomicInteger counter = new AtomicInteger();

	/**
	 * @param connectionFactory The connection factory.
	 * @see AbstractMessageListenerContainer#setConnectionFactory(ConnectionFactory)
	 */
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	/**
	 * @param errorHandler The error handler.
	 * @see AbstractMessageListenerContainer#setErrorHandler(org.springframework.util.ErrorHandler)
	 */
	public void setErrorHandler(ErrorHandler errorHandler) {
		this.errorHandler = errorHandler;
	}

	/**
	 * @param messageConverter the message converter to use
	 * @see AbstractMessageListenerContainer#setMessageConverter(MessageConverter)
	 */
	public void setMessageConverter(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	/**
	 * @param acknowledgeMode the acknowledge mode to set. Defaults to {@link AcknowledgeMode#AUTO}
	 * @see AbstractMessageListenerContainer#setAcknowledgeMode(AcknowledgeMode)
	 */
	public void setAcknowledgeMode(AcknowledgeMode acknowledgeMode) {
		this.acknowledgeMode = acknowledgeMode;
	}

	/**
	 * @param channelTransacted the flag value to set
	 * @see AbstractMessageListenerContainer#setChannelTransacted
	 */
	public void setChannelTransacted(Boolean channelTransacted) {
		this.channelTransacted = channelTransacted;
	}

	/**
	 * @param autoStartup true for auto startup.
	 * @see AbstractMessageListenerContainer#setAutoStartup(boolean)
	 */
	public void setAutoStartup(Boolean autoStartup) {
		this.autoStartup = autoStartup;
	}

	/**
	 * @param phase The phase.
	 * @see AbstractMessageListenerContainer#setPhase(int)
	 */
	public void setPhase(int phase) {
		this.phase = phase;
	}


	@Override
	public C createListenerContainer(RabbitListenerEndpoint endpoint) {
		C instance = createContainerInstance();

		if (this.connectionFactory != null) {
			instance.setConnectionFactory(this.connectionFactory);
		}
		if (this.errorHandler != null) {
			instance.setErrorHandler(this.errorHandler);
		}
		if (this.messageConverter != null) {
			instance.setMessageConverter(this.messageConverter);
		}
		if (this.acknowledgeMode != null) {
			instance.setAcknowledgeMode(this.acknowledgeMode);
		}
		if (this.channelTransacted != null) {
			instance.setChannelTransacted(this.channelTransacted);
		}
		if (this.autoStartup != null) {
			instance.setAutoStartup(this.autoStartup);
		}
		if (this.phase != null) {
			instance.setPhase(this.phase);
		}
		instance.setListenerId(endpoint.getId());

		endpoint.setupListenerContainer(instance);
		initializeContainer(instance);

		return instance;
	}

	/**
	 * Create an empty container instance.
	 * @return the new container instance.
	 */
	protected abstract C createContainerInstance();

	/**
	 * Further initialize the specified container.
	 * <p>Subclasses can inherit from this method to apply extra
	 * configuration if necessary.
	 * @param instance the containe instance to configure.
	 */
	protected void initializeContainer(C instance) {
	}

}
