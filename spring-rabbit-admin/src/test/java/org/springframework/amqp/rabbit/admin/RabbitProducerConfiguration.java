package org.springframework.amqp.rabbit.admin;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitProducerConfiguration extends TestRabbitConfiguration {
	
	
	public RabbitProducerConfiguration() {
		super();
	}

	@Bean
	public Queue fooQueue() {		
		return declare(new Queue(TestConstants.QUEUE_NAME));
				
	}
		
	@Bean
	public DirectExchange directExchange()
	{
		// This is the same as the default exchanage.  TODO consider predefined 
		return declare(DirectExchange.DEFAULT); 
	}
	
	@Bean 
	public Binding fooBinding()
	{		
		return declare(new Binding(fooQueue(), directExchange(), TestConstants.ROUTING_KEY));		
	}
}
