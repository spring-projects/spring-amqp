package org.springframework.amqp.rabbit.admin;

import org.springframework.amqp.rabbit.core.Constants;
import org.springframework.amqp.rabbit.core.PojoHandler;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConsumerConfiguration extends TestRabbitConfiguration  {

	@Bean 
	public SimpleMessageListenerContainer simpleMessageListenerContainer() {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory());
		container.setQueueName(Constants.QUEUE_NAME);
		container.setConcurrentConsumers(5);
		
		MessageListenerAdapter adapter = new MessageListenerAdapter();
		adapter.setDelegate(new PojoHandler());		
		container.setMessageListener(adapter);
		
		return container;
	}
}
