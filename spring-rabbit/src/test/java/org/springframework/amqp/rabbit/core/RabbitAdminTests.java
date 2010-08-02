package org.springframework.amqp.rabbit.core;

import static org.junit.Assert.fail;

import org.junit.Test;

public class RabbitAdminTests {

	@Test
	public void testSettingOfNullRabbitTemplate() {
		RabbitTemplate rabbitTemplate = null;
		try {
			new RabbitAdmin(rabbitTemplate);
			fail("should have thrown IllegalStateException when RabbitTemplate is not set.");
		}
		catch (IllegalArgumentException e) {
			
		}
	}

}
