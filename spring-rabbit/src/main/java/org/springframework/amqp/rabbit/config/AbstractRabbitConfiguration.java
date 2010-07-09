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

import java.util.Collection;

import org.springframework.amqp.config.AbstractAmqpConfiguration;
import org.springframework.amqp.core.AbstractExchange;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.ChannelCallback;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP.Queue.DeclareOk;

/**
 * Abstract base class for code based configuration of Spring managed Rabbit broker infrastructure,
 * i.e. Queues, Exchanges, Bindings.
 * <p>Subclasses are required to provide an implementation of rabbitTemplate from which the the bean 
 * 'amqpAdmin' will be created.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 */
@Configuration
public abstract class AbstractRabbitConfiguration extends AbstractAmqpConfiguration implements ApplicationContextAware, SmartLifecycle {

	private volatile AmqpAdmin amqpAdmin;

	private volatile ApplicationContext applicationContext;

	private volatile boolean running;


	@Bean 
	public abstract RabbitTemplate rabbitTemplate();

	@Bean
	public AmqpAdmin amqpAdmin() {
		this.amqpAdmin = new RabbitAdmin(rabbitTemplate());
		return this.amqpAdmin;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}

	/**
	 * Create a queue declaration with a name generated from the RabbitMQ broker,   
	 * exclusive=true, auto-delete = true, and durable = false;
	 * @return
	 */
	public Queue randomNameQueueDefinition() {
		DeclareOk declareOk = rabbitTemplate().execute(new ChannelCallback<DeclareOk>() {
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


	// SmartLifecycle implementation

	public boolean isAutoStartup() {
		return true;
	}

	public boolean isRunning() {
		return this.running;
	}

	@Override
	public void start() {
		synchronized (this) {
			if (this.running) {
				return;
			}
			Collection<AbstractExchange> exchanges = this.applicationContext.getBeansOfType(AbstractExchange.class).values();
			for (AbstractExchange exchange : exchanges) {
				this.amqpAdmin.declareExchange(exchange);
			}
			Collection<Queue> queues = this.applicationContext.getBeansOfType(Queue.class).values();
			for (Queue queue : queues) {
				this.amqpAdmin.declareQueue(queue);
			}
			Collection<Binding> bindings = this.applicationContext.getBeansOfType(Binding.class).values();
			for (Binding binding : bindings) {
				this.amqpAdmin.declareBinding(binding);
			}
			this.running = true;
		}
	}

	public void stop() {
	}

	public void stop(Runnable callback) {
	}

	public int getPhase() {
		return Integer.MIN_VALUE;
	}

}
