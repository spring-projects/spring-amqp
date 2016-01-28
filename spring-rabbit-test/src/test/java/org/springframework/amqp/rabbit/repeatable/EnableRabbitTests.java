/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.amqp.rabbit.repeatable;


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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

	@Rule
	public final ExpectedException thrown = ExpectedException.none();

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
