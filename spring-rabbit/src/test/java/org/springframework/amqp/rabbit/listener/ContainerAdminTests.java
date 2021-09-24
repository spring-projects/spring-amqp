/*
 * Copyright 2021 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.utils.test.TestUtils;
import org.springframework.context.support.GenericApplicationContext;

/**
 * @author Gary Russell
 * @since 2.4
 *
 */
@RabbitAvailable
public class ContainerAdminTests {

	@Test
	void findAdminInParentContext() {
		GenericApplicationContext parent = new GenericApplicationContext();
		CachingConnectionFactory cf =
				new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		RabbitAdmin admin = new RabbitAdmin(cf);
		parent.registerBean(RabbitAdmin.class, () -> admin);
		parent.refresh();
		GenericApplicationContext child = new GenericApplicationContext(parent);
		SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(cf);
		child.registerBean(SimpleMessageListenerContainer.class, () -> container);
		child.refresh();
		container.start();
		assertThat(TestUtils.getPropertyValue(container, "amqpAdmin")).isSameAs(admin);
		container.stop();
	}

}
