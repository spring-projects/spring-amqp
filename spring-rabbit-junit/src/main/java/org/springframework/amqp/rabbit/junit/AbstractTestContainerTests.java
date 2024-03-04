/*
 * Copyright 2021-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.junit;

import java.io.IOException;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
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

	protected static final RabbitMQContainer RABBITMQ;

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
		RABBITMQ.start();
		RABBITMQ.execInContainer("rabbitmq-plugins", "enable", "rabbitmq_stream");
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
		return RABBITMQ.getHttpUrl() + "/api/";
	}

}
