/*
 * Copyright 2017-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * @author Gary Russell
 * @since 2.0.2
 *
 */
public class RabbitTemplateIntegrationPubCFTests extends RabbitTemplateIntegrationTests {

	@Override
	@BeforeEach
	public void create(TestInfo info) {
		super.create(info);
		this.template.setUsePublisherConnection(true);
		this.routingTemplate.setUsePublisherConnection(true);
	}

}
