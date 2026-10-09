/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import org.springframework.amqp.rabbit.connection.ConnectionFactory;

/**
 * @author Gary Russell
 * @since 2.0.2
 *
 */
public class RabbitTemplateDirectReplyToContainerIntegrationPubCFTests
		extends RabbitTemplateDirectReplyToContainerIntegrationTests {

	@Override
	protected RabbitTemplate createSendAndReceiveRabbitTemplate(ConnectionFactory connectionFactory) {
		RabbitTemplate srTemplate = super.createSendAndReceiveRabbitTemplate(connectionFactory);
		srTemplate.setUsePublisherConnection(true);
		return srTemplate;
	}

}
