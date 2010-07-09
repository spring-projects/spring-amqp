package org.springframework.amqp.helloworld.async;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class Producer {

	public static void main(String[] args) throws Exception {
		new AnnotationConfigApplicationContext(ProducerConfiguration.class);
	}

}
