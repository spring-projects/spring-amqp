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

package org.springframework.amqp.rabbit.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.util.Assert;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

/**
 * RabbitMQ implementation of portable AMQP administrative operations for AMQP >= 0.9.1
 * 
 * @author Mark Pollack
 */
public class RabbitAdmin implements AmqpAdmin, InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private ConnectionFactory connectionFactory; 
	
	private RabbitTemplate rabbitTemplate;


	public RabbitAdmin() {
	}
	
	public RabbitAdmin(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
		Assert.notNull(this.connectionFactory, "ConnectionFactory is required");
		this.rabbitTemplate = new RabbitTemplate(connectionFactory);
		
	}
	

	public RabbitAdmin(RabbitTemplate rabbitTemplate) {
		this.rabbitTemplate = rabbitTemplate;
	}

	public RabbitTemplate getRabbitTemplate() {
		return this.rabbitTemplate;
	}

	public void afterPropertiesSet() {		
		if (connectionFactory == null) {
			throw new IllegalArgumentException("'connectionFactory' is required");
		}
		rabbitTemplate = new RabbitTemplate(connectionFactory);
	}

	// Exchange operations

	public void declareExchange(final Exchange exchange) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.exchangeDeclare(exchange.getName(), exchange.getExchangeType().name(),
						exchange.isDurable(), exchange.isAutoDelete(), exchange.getArguments());
				return null;
			}
		});
	}

	@ManagedOperation
	public void deleteExchange(final String exchangeName) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.exchangeDelete(exchangeName);
				return null;
			}
		});
	}

	// Queue operations

	@ManagedOperation
	public void declareQueue(final Queue queue) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDeclare(queue.getName(), queue.isDurable(), queue
						.isExclusive(), queue.isAutoDelete(), queue
						.getArguments());
				return null;
			}
		});
	}
	
	@ManagedOperation
	public Queue declareQueue() {
		DeclareOk declareOk = rabbitTemplate.execute(new ChannelCallback<DeclareOk>() {
			public DeclareOk doInRabbit(Channel channel) throws Exception {
				return channel.queueDeclare();
			}
		});			
		Queue queue = new Queue(declareOk.getQueue());
		queue.setExclusive(true);
		queue.setAutoDelete(true);
		queue.setDurable(false);
		return queue;
	}

	@ManagedOperation
	public void deleteQueue(final String queueName) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDelete(queueName);
				return null;
			}
		});
	}

	@ManagedOperation
	public void deleteQueue(final String queueName, final boolean unused, final boolean empty) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queueDelete(queueName, unused, empty);
				return null;
			}
		});
	}

	@ManagedOperation
	public void purgeQueue(final String queueName, final boolean noWait) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				channel.queuePurge(queueName, noWait);
				return null;
			}
		});
	}

	// Binding
	@ManagedOperation
	public void declareBinding(final Binding binding) {
		rabbitTemplate.execute(new ChannelCallback<Object>() {
			public Object doInRabbit(Channel channel) throws Exception {
				logger.debug("Binding queue to exchange with routing key");
				channel.queueBind(binding.getQueue(), binding.getExchange(),
						binding.getRoutingKey(), binding.getArguments());
				return null;
			}
		});
	}

}
