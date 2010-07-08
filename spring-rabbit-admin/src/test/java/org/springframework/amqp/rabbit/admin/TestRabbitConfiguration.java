package org.springframework.amqp.rabbit.admin;

import org.springframework.amqp.rabbit.config.AbstractRabbitConfiguration;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;

public class TestRabbitConfiguration extends AbstractRabbitConfiguration {
	
	@Bean 
	public RabbitTemplate rabbitTemplate() {
		RabbitTemplate template = new RabbitTemplate(connectionFactory());
		template.setExchange(TestConstants.EXCHANGE_NAME);
		template.setRoutingKey(TestConstants.ROUTING_KEY);	
		return template;
	}

	@Bean
	public ConnectionFactory connectionFactory() {
		//TODO make it possible to customize in subclasses.
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		connectionFactory.setChannelCacheSize(10);
		return connectionFactory;
	}

}
