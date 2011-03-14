package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

public class RabbitAdminTests {

	@Test
	public void testSettingOfNullRabbitTemplate() {
		ConnectionFactory connectionFactory = null;
		try {
			new RabbitAdmin(connectionFactory);
			fail("should have thrown IllegalStateException when RabbitTemplate is not set.");
		}
		catch (IllegalArgumentException e) {
			
		}
	}

}
