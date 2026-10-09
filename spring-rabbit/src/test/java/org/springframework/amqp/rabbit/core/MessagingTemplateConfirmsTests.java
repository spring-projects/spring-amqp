/*
 * Copyright 2020-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory.ConfirmType;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.support.GenericMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @since 2.3
 *
 */
@RabbitAvailable("messaging.confirms")
public class MessagingTemplateConfirmsTests {

	@Test
	void confirmHeader() throws Exception {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		RabbitTemplate rt = new RabbitTemplate(ccf);
		RabbitMessagingTemplate rmt = new RabbitMessagingTemplate(rt);
		CorrelationData data = new CorrelationData();
		rmt.send("messaging.confirms",
				new GenericMessage<>("foo", Collections.singletonMap(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, data)));
		assertThat(data.getFuture().get(10, TimeUnit.SECONDS).ack()).isTrue();
		ccf.destroy();
	}

	@Test
	void confirmHeaderUnroutable() throws Exception {
		CachingConnectionFactory ccf = new CachingConnectionFactory(
				RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		ccf.setPublisherConfirmType(ConfirmType.CORRELATED);
		ccf.setPublisherReturns(true);
		RabbitTemplate rt = new RabbitTemplate(ccf);
		rt.setMandatory(true);
		RabbitMessagingTemplate rmt = new RabbitMessagingTemplate(rt);
		CorrelationData data = new CorrelationData("foo");
		rmt.send("messaging.confirms.unroutable",
				new GenericMessage<>("foo", Collections.singletonMap(AmqpHeaders.PUBLISH_CONFIRM_CORRELATION, data)));
		assertThat(data.getFuture().get(10, TimeUnit.SECONDS).ack()).isTrue();
		assertThat(data.getReturned()).isNotNull();
		ccf.destroy();
	}

}
