/*
 * Copyright 2022-present the original author or authors.
 */

package org.springframework.amqp.rabbit.config;

import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.ListenerContainerFactoryBean.Type;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.utils.test.TestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
