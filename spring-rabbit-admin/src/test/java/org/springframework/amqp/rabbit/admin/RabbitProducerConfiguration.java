package org.springframework.amqp.rabbit.admin;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitProducerConfiguration extends TestRabbitConfiguration {

	@Bean
	public Queue fooQueue() {		
		return new Queue(TestConstants.QUEUE_NAME);
	}

	@Bean 
	public Binding fooBinding() {		
		return new Binding(fooQueue(), defaultExchange(), TestConstants.ROUTING_KEY);		
	}

}
