/*
 * Copyright 2021-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

import java.io.IOException;
import java.time.Duration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.rabbitmq.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.4
 *
 */
@Testcontainers(disabledWithoutDocker = true)
public abstract class AbstractTestContainerTests {

	private static final Log LOG = LogFactory.getLog(AbstractTestContainerTests.class);

	protected static final @Nullable RabbitMQContainer RABBITMQ;

	static {
		if (System.getProperty("spring.rabbit.use.local.server") == null
				&& System.getenv("SPRING_RABBIT_USE_LOCAL_SERVER") == null) {
			String image = "rabbitmq:management";
			String cache = System.getenv().get("IMAGE_CACHE");
			if (cache != null) {
				image = cache + image;
			}
			DockerImageName imageName = DockerImageName.parse(image)
					.asCompatibleSubstituteFor("rabbitmq");
			RABBITMQ = new RabbitMQContainer(imageName)
					.withExposedPorts(5672, 15672, 5552)
					.withStartupTimeout(Duration.ofMinutes(2));
		}
		else {
			RABBITMQ = null;
		}
	}

	@BeforeAll
	static void startContainer() throws IOException, InterruptedException {
		if (RABBITMQ != null) {
			RABBITMQ.start();
			RABBITMQ.execInContainer("rabbitmq-plugins", "enable", "rabbitmq_stream");
		}
		else {
			LOG.info("The local RabbitMQ broker will be used instead of Testcontainers.");
		}
	}

	public static int amqpPort() {
		return RABBITMQ != null ? RABBITMQ.getAmqpPort() : 5672;
	}

	public static int managementPort() {
		return RABBITMQ != null ? RABBITMQ.getMappedPort(15672) : 15672;
	}

	public static int streamPort() {
		return RABBITMQ != null ? RABBITMQ.getMappedPort(5552) : 5552;
	}

	public static String restUri() {
		return RABBITMQ != null ? RABBITMQ.getHttpUrl() + "/api/" : "http://localhost:" + managementPort() + "/api/";
	}

}
