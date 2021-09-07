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

package org.springframework.rabbit.stream.listener;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.config.StreamRabbitListenerContainerFactory;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.rabbitmq.http.client.Client;
import com.rabbitmq.http.client.domain.QueueInfo;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;

/**
 * @author Gary Russell
 * @since 2.4
 *
 */
@SpringJUnitConfig
@DirtiesContext
public class RabbitListenerTests extends AbstractIntegrationTests {

	@Autowired
	Config config;

	@Test
	void simple(@Autowired RabbitStreamTemplate template) throws Exception {
		Future<Boolean> future = template.convertAndSend("foo");
		assertThat(future.get(10, TimeUnit.SECONDS)).isTrue();
		future = template.convertAndSend("bar", msg -> msg);
		assertThat(future.get(10, TimeUnit.SECONDS)).isTrue();
		future = template.send(new org.springframework.amqp.core.Message("baz".getBytes(),
				new StreamMessageProperties()));
		assertThat(future.get(10, TimeUnit.SECONDS)).isTrue();
		future = template.send(template.messageBuilder().addData("qux".getBytes()).build());
		assertThat(future.get(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.received).containsExactly("foo", "bar", "baz", "qux");
		assertThat(this.config.id).isEqualTo("test");
	}

	@Test
	void nativeMsg(@Autowired RabbitTemplate template) throws InterruptedException {
		template.convertAndSend("test.stream.queue2", "foo");
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.receivedNative).isNotNull();
		assertThat(this.config.context).isNotNull();
	}

	@Test
	void queueOverAmqp() throws Exception {
		Client client = new Client("http://guest:guest@localhost:" + RABBITMQ.getMappedPort(15672) + "/api");
		QueueInfo queue = client.getQueue("/", "stream.created.over.amqp");
		assertThat(queue.getArguments().get("x-queue-type")).isEqualTo("stream");
	}

	@Configuration(proxyBeanMethods = false)
	@EnableRabbit
	public static class Config {

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(1);

		final List<String> received = new ArrayList<>();

		volatile Message receivedNative;

		volatile Context context;

		volatile String id;

		@Bean
		Environment environment() {
			return Environment.builder()
					.addressResolver(add -> new Address("localhost", RABBITMQ.getMappedPort(5552)))
					.build();
		}

		@Bean
		SmartLifecycle creator(Environment env) {
			return new SmartLifecycle() {

				@Override
				public void stop() {
				}

				@Override
				public void start() {
					env.streamCreator().stream("test.stream.queue1").create();
					env.streamCreator().stream("test.stream.queue2").create();
				}

				@Override
				public boolean isRunning() {
					return false;
				}
			};
		}

		@Bean
		RabbitListenerContainerFactory<StreamListenerContainer> rabbitListenerContainerFactory(Environment env) {
			return new StreamRabbitListenerContainerFactory(env);
		}

		@RabbitListener(queues = "test.stream.queue1")
		void listen(String in) {
			this.received.add(in);
			this.latch1.countDown();
		}

		@Bean
		RabbitListenerContainerFactory<StreamListenerContainer> nativeFactory(Environment env) {
			StreamRabbitListenerContainerFactory factory = new StreamRabbitListenerContainerFactory(env);
			factory.setNativeListener(true);
			factory.setConsumerCustomizer((id, builder) -> {
				builder.name("myConsumer")
						.offset(OffsetSpecification.first())
						.manualTrackingStrategy();
				this.id = id;
			});
			return factory;
		}

		@RabbitListener(id = "test", queues = "test.stream.queue2", containerFactory = "nativeFactory")
		void nativeMsg(Message in, Context context) {
			this.receivedNative = in;
			this.context = context;
			this.latch2.countDown();
			context.storeOffset();
		}

		@Bean
		CachingConnectionFactory cf() {
			return new CachingConnectionFactory(RABBITMQ.getContainerIpAddress(), RABBITMQ.getFirstMappedPort());
		}

		@Bean
		RabbitTemplate template(CachingConnectionFactory cf) {
			return new RabbitTemplate(cf);
		}

		@Bean
		RabbitStreamTemplate streamTemplate1(Environment env) {
			RabbitStreamTemplate template = new RabbitStreamTemplate(env, "test.stream.queue1");
			template.setProducerCustomizer((name, builder) -> builder.name("test"));
			return template;
		}

		@Bean
		RabbitAdmin admin(CachingConnectionFactory cf) {
			return new RabbitAdmin(cf);
		}

		@Bean
		Queue queue() {
			return QueueBuilder.durable("stream.created.over.amqp")
					.stream()
					.build();
		}

 	}

}
