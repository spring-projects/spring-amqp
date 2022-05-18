/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean.Type;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;

/**
 * @author Gary Russell
 * @since 2.4.6
 *
 */
public class ListenerContainerFactoryBeanTests {

	@SuppressWarnings("unchecked")
	@Test
	void micrometer() throws Exception {
		ListenerContainerFactoryBean lcfb = new ListenerContainerFactoryBean();
		lcfb.setConnectionFactory(mock(ConnectionFactory.class));
		lcfb.setMicrometerEnabled(false);
		lcfb.setMicrometerTags(Map.of("foo", "bar"));
		lcfb.afterPropertiesSet();
		AbstractMessageListenerContainer container = lcfb.getObject();
		assertThat(TestUtils.getPropertyValue(container, "micrometerEnabled", Boolean.class)).isFalse();
		assertThat(TestUtils.getPropertyValue(container, "micrometerTags", Map.class)).hasSize(1);
	}

	@Test
	void smlcCustomizer() throws Exception {
		ListenerContainerFactoryBean lcfb = new ListenerContainerFactoryBean();
		lcfb.setConnectionFactory(mock(ConnectionFactory.class));
		lcfb.setSMLCCustomizer(container -> {
			container.setConsumerStartTimeout(42L);
		});
		lcfb.afterPropertiesSet();
		AbstractMessageListenerContainer container = lcfb.getObject();
		assertThat(TestUtils.getPropertyValue(container, "consumerStartTimeout", Long.class)).isEqualTo(42L);
	}

	@Test
	void dmlcCustomizer() throws Exception {
		ListenerContainerFactoryBean lcfb = new ListenerContainerFactoryBean();
		lcfb.setConnectionFactory(mock(ConnectionFactory.class));
		lcfb.setType(Type.direct);
		lcfb.setDMLCCustomizer(container -> {
			container.setConsumersPerQueue(2);
		});
		lcfb.afterPropertiesSet();
		AbstractMessageListenerContainer container = lcfb.getObject();
		assertThat(TestUtils.getPropertyValue(container, "consumersPerQueue", Integer.class)).isEqualTo(2);
	}


}
