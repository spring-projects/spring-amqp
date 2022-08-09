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

package org.springframework.rabbit.stream.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.rabbit.stream.config.SuperStream;
import org.springframework.rabbit.stream.support.AbstractIntegrationTests;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@SpringJUnitConfig
@Disabled
public class SuperStreamSACTests extends AbstractIntegrationTests {

	@Test
	void superStream(@Autowired ApplicationContext context, @Autowired RabbitTemplate template,
			@Autowired Environment env, @Autowired Config config, @Autowired RabbitAdmin admin,
			@Autowired Declarables declarables) throws InterruptedException {

		template.getConnectionFactory().createConnection();
		StreamListenerContainer container1 = context.getBean(StreamListenerContainer.class, env, "one");
		container1.start();
		StreamListenerContainer container2 = context.getBean(StreamListenerContainer.class, env, "two");
		container2.start();
		StreamListenerContainer container3 = context.getBean(StreamListenerContainer.class, env, "three");
		container3.start();
		template.convertAndSend("ss.sac.test", "0", "foo");
		template.convertAndSend("ss.sac.test", "1", "bar");
		template.convertAndSend("ss.sac.test", "2", "baz");
		assertThat(config.latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(config.messages.keySet()).contains("one", "two", "three");
		assertThat(config.info).contains("one:foo", "two:bar", "three:baz");
		container1.stop();
		container2.stop();
		container3.stop();
		clean(admin, declarables);
	}

	private void clean(RabbitAdmin admin, Declarables declarables) {
		declarables.getDeclarablesByType(Queue.class).forEach(queue -> admin.deleteQueue(queue.getName()));
		declarables.getDeclarablesByType(DirectExchange.class).forEach(ex -> admin.deleteExchange(ex.getName()));
	}

	@Configuration
	public static class Config {

		final List<String> info = new ArrayList<>();

		final Map<String, Message> messages = new ConcurrentHashMap<>();

		final CountDownLatch latch = new CountDownLatch(3);

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
			return new SuperStream("ss.sac.test", 3);
		}

		@Bean
		static Environment environment() {
			return Environment.builder()
					.addressResolver(add -> new Address("localhost", streamPort()))
					.build();
		}

		@Bean
		@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
		StreamListenerContainer container(Environment env, String name) {
			StreamListenerContainer container = new StreamListenerContainer(env);
			container.superStream("ss.sac.test", "test");
			container.setupMessageListener(msg -> {
				this.messages.put(name, msg);
				this.info.add(name + ":" + new String(msg.getBody()));
				this.latch.countDown();
			});
			container.setConsumerCustomizer((id, builder) -> builder.offset(OffsetSpecification.last()));
			container.setAutoStartup(false);
			return container;
		}

	}

}
