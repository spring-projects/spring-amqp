/*
 * Copyright 2016-present the original author or authors.
 */

package org.springframework.amqp.rabbit.repeatable;


import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.config.RabbitListenerContainerTestFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Stephane Nicoll
 * @author Gary Russell
 *
 * @since 1.6
 *
 */
public class EnableRabbitTests extends AbstractRabbitAnnotationDrivenTests {

	@Override
	@Test
	public void rabbitListenerIsRepeatable() {
		ConfigurableApplicationContext context = new AnnotationConfigApplicationContext(
				EnableRabbitDefaultContainerFactoryConfig.class,
				RabbitListenerRepeatableBean.class,
				ClassLevelRepeatableBean.class);
		testRabbitListenerRepeatable(context);
	}

	@Configuration
	@EnableRabbit
	static class EnableRabbitDefaultContainerFactoryConfig {

		@Bean
		public RabbitListenerContainerTestFactory rabbitListenerContainerFactory() {
			return new RabbitListenerContainerTestFactory();
		}
	}

}
