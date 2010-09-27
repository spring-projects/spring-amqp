package org.springframework.amqp.helloworld;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class BrokerConfigurationApplication {

	/**
	 * An example application that only configures the AMQP broker
	 */
	public static void main(String[] args) throws Exception {
		ApplicationContext context = new ClassPathXmlApplicationContext("rabbitConfiguration.xml");
		AmqpAdmin amqpAdmin = context.getBean(AmqpAdmin.class);
		Queue helloWorldQueue = new Queue("hello.world.queue");
		
		amqpAdmin.declareQueue(helloWorldQueue);

	}

}
