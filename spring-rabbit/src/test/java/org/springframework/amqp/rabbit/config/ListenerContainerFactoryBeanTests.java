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
 * @author Artem Bilan
 *
 * @since 2.4.6
 *
 */
public class ListenerContainerFactoryBeanTests {

	@Test
	void micrometer() throws Exception {
		ListenerContainerFactoryBean lcfb = new ListenerContainerFactoryBean();
		lcfb.setConnectionFactory(mock(ConnectionFactory.class));
		lcfb.setMicrometerEnabled(false);
		lcfb.setMicrometerTags(Map.of("foo", "bar"));
		lcfb.afterPropertiesSet();
		AbstractMessageListenerContainer container = lcfb.getObject();
		assertThat(TestUtils.<Boolean>getPropertyValue(container, "micrometerEnabled")).isFalse();
		assertThat(TestUtils.<Map<?, ?>>getPropertyValue(container, "micrometerTags")).hasSize(1);
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
		assertThat(TestUtils.<Long>getPropertyValue(container, "consumerStartTimeout")).isEqualTo(42L);
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
		assertThat(TestUtils.<Integer>getPropertyValue(container, "consumersPerQueue")).isEqualTo(2);
	}

}
