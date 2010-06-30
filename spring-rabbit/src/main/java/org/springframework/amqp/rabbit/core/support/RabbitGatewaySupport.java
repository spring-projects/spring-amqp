/*
 * Copyright 2002-2010 the original author or authors.
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


package org.springframework.amqp.rabbit.core.support;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;

/**
 * Convenient super class for application classes that need JMS access.
 * 
 * <p>Requires a ConnectionFactory or a RabbitTemplate instance to be set.
 * It will create its own RabbitTemplate if a ConnectionFactory is passed in.
 * A custom RabbitTemplate instance can be created for a given ConnectionFactory
 * through overriding the {@link #createRabbitTemplate} method.
 *
 * @author Mark Pollack
 * @see #setConnectionFactory
 * @see #setRabbitTemplate
 * @see #createRabbitTemplate
 * @see org.springframework.amqp.rabbit.core.RabbitTemplate
 */
public class RabbitGatewaySupport implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());
	
	private RabbitTemplate rabbitTemplate;
	
	/**
	 * Set the Rabbit connection factory to be used by the gateway.
	 * Will automatically create a RabbitTemplate for the given ConnectionFactory.
	 * @see #createRabbitTemplate
	 * @see #setConnectionFactory(org.springframework.amqp.rabbit.connection.ConnectionFactory)
	 * @param connectionFactory
	 */
	public final void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.rabbitTemplate = createRabbitTemplate(connectionFactory);
	}
	
	/**
	 * Create a RabbitTemplate for the given ConnectionFactory.
	 * Only invoked if populating the gateway with a ConnectionFactory reference.
	 * @param connectionFactory the Rabbit ConnectionFactory to create a RabbitTemplate for
	 * @return the new RabbitTemplate instance
	 * @see #setConnectionFactory
	 */
	protected RabbitTemplate createRabbitTemplate(ConnectionFactory connectionFactory) {
		return new RabbitTemplate(connectionFactory);
	}
	
	/**
	 * Return the Rabbit ConnectionFactory used by the gateway.
	 */
	public final ConnectionFactory getConnectionFactory() {
		return (this.rabbitTemplate != null ? this.rabbitTemplate.getConnectionFactory() : null);
	}
	
	/**
	 * Set the RabbitTemplate for the gateway.
	 * @param rabbitTemplate
	 * @see #setConnectionFactory(org.springframework.amqp.rabbit.connection.ConnectionFactory)
	 */
	public final void setRabbitTemplate(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}
	
	/**
	 * Return the RabbitTemplate for the gateway.
	 */
	public final RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}
	
	public final void afterPropertiesSet() throws IllegalArgumentException, BeanInitializationException {
		if (this.rabbitTemplate == null) {
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
	 * @throws java.lang.Exception if initialization fails
	 */
	protected void initGateway() throws Exception {
	}

}
