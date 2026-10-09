/*
 * Copyright 2018-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
@RabbitAvailable(queues = SimplePublisherConfirmsTests.QUEUE)
public class SimplePublisherConfirmsTests {

	public static final String QUEUE = "simple.confirms";

	@Test
	public void testConfirms() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		cf.setPublisherConfirmType(ConfirmType.SIMPLE);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setRoutingKey(QUEUE);
		Boolean invokeResult = template.invoke(t -> {
			template.convertAndSend("foo");
			template.convertAndSend("bar");
			template.waitForConfirmsOrDie(10_000);
			return true;
		});
		assertThat(invokeResult).isTrue();
		cf.destroy();
	}

	@Test
	public void testConfirmsWithCallbacks() {
		CachingConnectionFactory cf = new CachingConnectionFactory("localhost");
		cf.setPublisherConfirmType(ConfirmType.SIMPLE);
		RabbitTemplate template = new RabbitTemplate(cf);
		template.setRoutingKey(QUEUE);
		AtomicReference<MessageProperties> finalProperties = new AtomicReference<>();
		AtomicLong lastAck = new AtomicLong();
		Boolean invokeResult = template.invoke(t -> {
			template.convertAndSend("foo");
			template.convertAndSend("bar", m -> {
				finalProperties.set(m.getMessageProperties());
				return m;
			});
			template.waitForConfirmsOrDie(10_000);
			return true;
		}, (tag, multiple) -> {
			lastAck.set(tag);
		}, (tag, multiple) -> {
		});
		assertThat(invokeResult).isTrue();
		assertThat(lastAck.get()).isEqualTo(finalProperties.get().getPublishSequenceNumber());
		cf.destroy();
	}

}
