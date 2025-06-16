/*
 * Copyright 2022-present the original author or authors.
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

package org.springframework.rabbit.stream.listener;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.config.SuperStream;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 3.0
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class SuperStreamConcurrentSACTests extends AbstractTestContainerTests {

	@Test
	void concurrent(@Autowired StreamListenerContainer container, @Autowired RabbitTemplate template,
			@Autowired Config config, @Autowired RabbitAdmin admin,
			@Autowired Declarables superStream) throws InterruptedException {

		template.getConnectionFactory().createConnection();
		container.start();
		assertThat(config.consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();
		template.convertAndSend("ss.sac.concurrency.test", "0", "foo");
		template.convertAndSend("ss.sac.concurrency.test", "1", "bar");
		template.convertAndSend("ss.sac.concurrency.test", "2", "baz");
		assertThat(config.messageLatch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.threads).hasSize(3);
		container.stop();
		clean(admin, superStream);
	}

	private void clean(RabbitAdmin admin, Declarables declarables) {
		declarables.getDeclarablesByType(Queue.class).forEach(queue -> admin.deleteQueue(queue.getName()));
		declarables.getDeclarablesByType(DirectExchange.class).forEach(ex -> admin.deleteExchange(ex.getName()));
	}

	@Configuration
	public static class Config {

		final Set<String> threads = new HashSet<>();

		final CountDownLatch consumerLatch = new CountDownLatch(3);

		final CountDownLatch messageLatch = new CountDownLatch(3);

		@Bean
		CachingConnectionFactory cf() {
			return new CachingConnectionFactory("localhost", amqpPort());
		}

		@Bean
		RabbitAdmin admin(ConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		RabbitTemplate template(ConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

		@Bean
		SuperStream superStream() {
			return new SuperStream("ss.sac.concurrency.test", 3);
		}

		@Bean
		static Environment environment() {
			return Environment.builder()
					.port(streamPort())
					.maxConsumersByConnection(1)
					.build();
		}

		@Bean
		StreamListenerContainer concurrentContainer(Environment env) {
			StreamListenerContainer container = new StreamListenerContainer(env);
			container.superStream("ss.sac.concurrency.test", "concurrent", 3);
			container.setupMessageListener(msg -> {
				this.threads.add(Thread.currentThread().getName());
				this.messageLatch.countDown();
			});
			container.setConsumerCustomizer((id, builder) -> {
				builder.consumerUpdateListener(context -> {
					this.consumerLatch.countDown();
					return OffsetSpecification.last();
				});
			});
			container.setAutoStartup(false);
			return container;
		}

	}

}
