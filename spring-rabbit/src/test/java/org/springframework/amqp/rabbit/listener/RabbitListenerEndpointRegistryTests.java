/*
 * Copyright 2002-2025 the original author or authors.
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

package org.springframework.amqp.rabbit.listener;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerEndpoint;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/**
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 */
public class RabbitListenerEndpointRegistryTests {

	private final RabbitListenerEndpointRegistry registry = new RabbitListenerEndpointRegistry();

	private final RabbitListenerContainerTestFactory containerFactory = new RabbitListenerContainerTestFactory();

	@Test
	public void createWithNullEndpoint() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.registerListenerContainer(null, containerFactory));
	}

	@Test
	public void createWithNullEndpointId() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.registerListenerContainer(new SimpleRabbitListenerEndpoint(), containerFactory));
	}

	@Test
	public void createWithNullContainerFactory() {
		assertThatIllegalArgumentException()
			.isThrownBy(() -> registry.registerListenerContainer(createEndpoint("foo", "myDestination"), null));
	}

	@Test
	public void createWithDuplicateEndpointId() {
		registry.registerListenerContainer(createEndpoint("test", "queue"), containerFactory);

		assertThatIllegalStateException()
			.isThrownBy(() -> registry.registerListenerContainer(createEndpoint("test", "queue"), containerFactory));
	}

	private SimpleRabbitListenerEndpoint createEndpoint(String id, String queueName) {
		SimpleRabbitListenerEndpoint endpoint = new SimpleRabbitListenerEndpoint();
		endpoint.setId(id);
		endpoint.setQueueNames(queueName);
		return endpoint;
	}

}
