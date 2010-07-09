package org.springframework.amqp.helloworld;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Consumer {
	
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(RabbitConfiguration.class);
		RabbitTemplate rabbitTemplate = context.getBean(RabbitTemplate.class);
		System.out.println("Received: " + rabbitTemplate.receiveAndConvert());
	}

}
