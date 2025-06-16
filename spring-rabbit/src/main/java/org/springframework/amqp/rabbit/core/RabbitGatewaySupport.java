/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.rabbit.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.lang.Nullable;

/**
 * Convenient super class for application classes that need RabbitMQ access.
 *
 * <p>Requires a ConnectionFactory or a RabbitTemplate instance to be set.
 * It will create its own RabbitTemplate if a ConnectionFactory is passed in.
 * A custom RabbitTemplate instance can be created for a given ConnectionFactory
 * through overriding the {@link #createRabbitTemplate} method.
 *
 * @author Mark Pollack
 * @author Gary Russell
 *
 * @see #setConnectionFactory
 * @see #setRabbitOperations
 * @see #createRabbitTemplate
 * @see org.springframework.amqp.rabbit.core.RabbitTemplate
 */
public class RabbitGatewaySupport implements InitializingBean {

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass()); // NOSONAR

	private RabbitOperations rabbitOperations;

	/**
	 * Set the Rabbit connection factory to be used by the gateway.
	 * Will automatically create a RabbitTemplate for the given ConnectionFactory.
	 * @param connectionFactory The connection factory.
	 * @see #createRabbitTemplate
	 * @see #setConnectionFactory(org.springframework.amqp.rabbit.connection.ConnectionFactory)
	 */
	public final void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.rabbitOperations = createRabbitTemplate(connectionFactory);
	}

	/**
	 * Create a RabbitTemplate for the given ConnectionFactory.
	 * Only invoked if populating the gateway with a ConnectionFactory reference.
	 *
	 * @param connectionFactory the Rabbit ConnectionFactory to create a RabbitTemplate for
	 * @return the new RabbitTemplate instance
	 * @see #setConnectionFactory
	 */
	protected RabbitTemplate createRabbitTemplate(ConnectionFactory connectionFactory) {
		return new RabbitTemplate(connectionFactory);
	}

	/**
	 * @return The Rabbit ConnectionFactory used by the gateway.
	 */
	@Nullable
	public final ConnectionFactory getConnectionFactory() {
		return (this.rabbitOperations != null ? this.rabbitOperations.getConnectionFactory() : null);
	}

	/**
	 * Set the {@link RabbitOperations} for the gateway.
	 * @param rabbitOperations The Rabbit operations.
	 * @see #setConnectionFactory(org.springframework.amqp.rabbit.connection.ConnectionFactory)
	 */
	public final void setRabbitOperations(RabbitOperations rabbitOperations) {
		this.rabbitOperations = rabbitOperations;
	}

	/**
	 * @return The {@link RabbitOperations} for the gateway.
	 */
	public final RabbitOperations getRabbitOperations() {
		return this.rabbitOperations;
	}

	@Override
	public final void afterPropertiesSet() throws IllegalArgumentException, BeanInitializationException {
		if (this.rabbitOperations == null) {
			throw new IllegalArgumentException("'connectionFactory' or 'rabbitTemplate' is required");
		}
		try {
			initGateway();
		}
		catch (Exception ex) {
			throw new BeanInitializationException("Initialization of Rabbit gateway failed: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Subclasses can override this for custom initialization behavior.
	 * Gets called after population of this instance's bean properties.
	 */
	protected void initGateway() {
	}

}
