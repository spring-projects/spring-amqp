package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.SingleConnectionFactory;

public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SingleConnectionFactory connectionFactory = new SingleConnectionFactory("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");

		RabbitTemplate template = new RabbitTemplate();
		template.setConnectionFactory(connectionFactory);
		template.setChannelTransacted(true);
		template.afterPropertiesSet();

		final String routingKey = TestConstants.ROUTING_KEY;
		QueueUtils.declareTestQueue(template, routingKey);

		// send message		
		sendMessages(template, TestConstants.EXCHANGE_NAME, routingKey, TestConstants.NUM_MESSAGES);
	}

	private static void sendMessages(RabbitTemplate template,
			final String exchange, final String routingKey, int numMessages) {
		for (int i = 1; i <= numMessages; i++) {
			byte[] bytes = "testing".getBytes();
			MessageProperties properties = new MessageProperties();
			properties.getHeaders().put("float", new Float(3.14));
			Message message = new Message(bytes, properties);
			template.send(exchange, routingKey, message);
			System.out.println("sending " + i + "...");
		}
	}

}
