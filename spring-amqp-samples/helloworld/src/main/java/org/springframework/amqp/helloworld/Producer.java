package org.springframework.amqp.helloworld;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ApplicationContext ctx = new AnnotationConfigApplicationContext(RabbitConfiguration.class);

		RabbitTemplate rabbitTemplate = (RabbitTemplate) ctx.getBean(RabbitTemplate.class);
		rabbitTemplate.convertAndSend("Hello World");	
		System.out.println("Sent Hello World.");
	
	}
	
}
