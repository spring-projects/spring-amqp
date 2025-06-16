/*
 * Copyright 2016-present the original author or authors.
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

package org.springframework.amqp.rabbit.annotation;

import java.io.Serializable;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * @author Artem Bilan
 * @since 1.5.5
 */
@SpringJUnitConfig
@DirtiesContext
@RabbitAvailable
public class EnableRabbitCglibProxyTests {

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Test
	public void testCglibProxy() {
		this.rabbitTemplate.setReplyTimeout(600000);
		Foo foo = new Foo();
		foo.field = "foo";
		assertThat(this.rabbitTemplate.convertSendAndReceive("auto.exch.test", "auto.rk.test", foo))
				.isEqualTo("Reply: foo: AUTO.RK.TEST");
	}

	@Configuration
	@EnableRabbit
	@EnableTransactionManagement(proxyTargetClass = true)
	public static class Config {

		@Bean
		public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(rabbitConnectionFactory());
			return factory;
		}


		@Bean
		public TxService<?> txService() {
			return new TxServiceImpl();
		}

		@Bean
		public ConnectionFactory rabbitConnectionFactory() {
			CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
			connectionFactory.setHost("localhost");
			return connectionFactory;
		}

		@Bean
		public RabbitTemplate rabbitTemplate() {
			return new RabbitTemplate(rabbitConnectionFactory());
		}

		@Bean
		public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
			return new RabbitAdmin(connectionFactory);
		}

		@Bean
		public PlatformTransactionManager transactionManager() {
			return mock(PlatformTransactionManager.class);
		}

	}


	interface TxService<P> {

		String handle(P payload, String rk);

	}

	static class TxServiceImpl implements TxService<Foo> {

		@Override
		@Transactional
		@RabbitListener(bindings = @QueueBinding(
				value = @Queue,
				exchange = @Exchange(value = "auto.exch.test", autoDelete = "true"),
				key = "auto.rk.test")
		)
		public String handle(@Payload Foo foo, @Header("amqp_receivedRoutingKey") String rk) {
			return "Reply: " + foo.field + ": " + rk.toUpperCase();
		}

	}

	@SuppressWarnings("serial")
	static class Foo implements Serializable {

		public String field;

	}

}
