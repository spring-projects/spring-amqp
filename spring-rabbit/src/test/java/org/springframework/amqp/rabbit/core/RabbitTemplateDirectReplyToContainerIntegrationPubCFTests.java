/*
 * Copyright and licensing information for this source code can be found in
 * the NOTICE and LICENSE files distributed with this project.
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
