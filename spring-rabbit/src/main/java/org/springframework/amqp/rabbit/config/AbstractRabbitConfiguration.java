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

package org.springframework.amqp.rabbit.config;

import org.springframework.amqp.config.AbstractAmqpConfiguration;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Abstract base class for code based configuration of Spring managed Rabbit based broker infrastructure,
 * i.e. Queues, Exchanges, Bindings.
 * <p>Subclasses are required to provide an implementation of rabbitTemplate from which the the bean 
 * 'amqpAdmin' will be created.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 */
@Configuration
public abstract class AbstractRabbitConfiguration extends AbstractAmqpConfiguration {

	/**
	 * Create a bean definition for RabbitTemplate.  Since there are several properties
	 * one may want to set after creating a RabbitTemplate from a ConnectionFactory, this
	 * abstract method is provided to allow for that flexibility as compared to
	 * automatically creating a RabbitTemplate by specifying a ConnectionFactory.
	 */
	@Bean 
	public abstract RabbitTemplate rabbitTemplate();

	@Bean
	public AmqpAdmin amqpAdmin() {
		return new RabbitAdmin(rabbitTemplate().getConnectionFactory());
	}

}
