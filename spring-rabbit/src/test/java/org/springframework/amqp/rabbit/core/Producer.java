package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageCreator;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.TestMessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
//import org.springframework.otp.erlang.connection.ConnectionParameters;


public class Producer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		connectionFactory.setChannelCacheSize(10);

		RabbitTemplate template = new RabbitTemplate();
		template.setConnectionFactory(connectionFactory);
		template.setChannelTransacted(true);
		template.afterPropertiesSet();

		final String routingKey = Constants.ROUTING_KEY;
		QueueUtils.declareTestQueue(template, routingKey);

		// send message		
		sendMessages(template, Constants.EXCHANGE_NAME, routingKey, Constants.NUM_MESSAGES);
	}

	private static void sendMessages(RabbitTemplate template,
			final String exchange, final String routingKey, int numMessages) {
		for (int i = 1; i <= numMessages; i++) {
			template.send(exchange, routingKey, new MessageCreator() {
				public Message createMessage() {
					byte[] bytes = "testing".getBytes();
					MessageProperties properties = new TestMessageProperties();
					properties.getHeaders().put("float", new Float(3.14));
					//properties.getHeaders().put("object", new ConnectionParameters(null,null));
					return new Message(bytes, properties);
				}
			});
			System.out.println("sending " + i + "...");
		}
	}
}
