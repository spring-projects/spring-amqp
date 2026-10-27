/*
 * Copyright 2026-present the original author or authors.
 */

package org.springframework.amqp.rabbit.core;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies native delivery-time support introduced in RabbitMQ 4.4.
 *
 * @author 樊孔缘
 */
@Testcontainers(disabledWithoutDocker = true)
class NativeDeliveryTimeIntegrationTests {

	private static final String QUEUE = "test.native.delivery.time";

	@Container
	static final RabbitMQContainer RABBIT_MQ_CONTAINER =
			new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.4.0-management"));

	private final CachingConnectionFactory connectionFactory =
			new CachingConnectionFactory("localhost", RABBIT_MQ_CONTAINER.getAmqpPort());

	private final RabbitAdmin rabbitAdmin = new RabbitAdmin(this.connectionFactory);

	private final RabbitTemplate rabbitTemplate = new RabbitTemplate(this.connectionFactory);

	@BeforeEach
	void declareQuorumQueue() {
		this.rabbitAdmin.deleteQueue(QUEUE);
		this.rabbitAdmin.declareQueue(QueueBuilder.durable(QUEUE).quorum().build());
	}

	@AfterEach
	void cleanUp() {
		this.rabbitAdmin.deleteQueue(QUEUE);
		this.connectionFactory.destroy();
	}

	@Test
	void nativeDeliveryTimeDelaysAMessageForAQuorumQueue() {
		Instant deliveryTime = Instant.now().plusSeconds(3);
		MessageProperties properties = new MessageProperties();
		properties.setDeliveryTime(deliveryTime);
		Message message = MessageBuilder.withBody("delayed".getBytes(StandardCharsets.UTF_8))
				.andProperties(properties)
				.build();

		this.rabbitTemplate.send(QUEUE, message);

		assertThat(this.rabbitTemplate.receive(QUEUE)).isNull();
		Message received = this.rabbitTemplate.receive(QUEUE, 10_000);
		assertThat(received).isNotNull();
		assertThat(received.getMessageProperties().getDeliveryTime()).isEqualTo(deliveryTime);
		assertThat(received.getBody()).asString().isEqualTo("delayed");
	}

}
