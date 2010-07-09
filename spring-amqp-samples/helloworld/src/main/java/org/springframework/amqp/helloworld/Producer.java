package org.springframework.amqp.helloworld;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Producer {

	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(RabbitConfiguration.class);
		RabbitTemplate rabbitTemplate = context.getBean(RabbitTemplate.class);
		rabbitTemplate.convertAndSend("Hello World");
		System.out.println("Sent: Hello World");
	}

}
