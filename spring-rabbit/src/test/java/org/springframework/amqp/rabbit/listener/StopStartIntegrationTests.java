/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.listener;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.fail;

/**
 * @author Gary Russell
 * @author Gunnar Hillert
 * @since 1.1.3
 *
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class StopStartIntegrationTests {

	private static AtomicInteger deliveries = new AtomicInteger();

	private static int COUNT = 10000;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Autowired
	private SimpleMessageListenerContainer container;

	@BeforeAll
	@AfterAll
	public static void setupAndCleanUp() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		RabbitAdmin admin = new RabbitAdmin(cf);
		admin.deleteQueue("stop.start.queue");
		admin.deleteExchange("stop.start.exchange");
		cf.destroy();
	}

	@Test
	public void test() throws Exception {
		for (int i = 0; i < COUNT; i++) {
			rabbitTemplate.convertAndSend("foo" + i);
		}
		long t = System.currentTimeMillis();
		container.start();
		int n;
		int lastN = 0;
		while ((n = deliveries.get()) < COUNT) {
			Thread.sleep(2000);
			container.stop();
			container.start();
			if (System.currentTimeMillis() - t > 240000 && lastN == n) {
				fail("Only received " + deliveries.get());
			}
			lastN = n;
		}
	}

	public static class StopStartListener implements MessageListener {

		@Override
		public void onMessage(Message message) {
			deliveries.incrementAndGet();
		}

	}
}
