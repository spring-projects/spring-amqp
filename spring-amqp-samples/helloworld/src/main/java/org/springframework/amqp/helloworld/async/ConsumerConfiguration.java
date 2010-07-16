package org.springframework.amqp.helloworld.async;

import org.springframework.amqp.helloworld.HelloWorldConfiguration;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfiguration extends HelloWorldConfiguration {

	@Bean
	public SimpleMessageListenerContainer listenerContainer() {
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
		container.setConnectionFactory(connectionFactory());
		container.setQueueName(this.helloWorldQueueName);
		container.setMessageListener(new MessageListenerAdapter(new HelloWorldHandler()));
		return container;
	}

}
