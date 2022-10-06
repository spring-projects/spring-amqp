/*
 * Copyright 2021-2022 the original author or authors.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.rabbit.stream.config.StreamRabbitListenerContainerFactory;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.retry.StreamRetryOperationsInterceptorFactoryBean;
import org.springframework.rabbit.stream.support.AbstractIntegrationTests;
import org.springframework.rabbit.stream.support.StreamMessageProperties;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
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
		future = template.convertAndSend("bar", msg -> null);
		assertThat(future.get(10, TimeUnit.SECONDS)).isFalse();
		assertThat(this.config.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.received).containsExactly("foo", "foo", "bar", "baz", "qux");
		assertThat(this.config.id).isEqualTo("testNative");
	}

	@Test
	void nativeMsg(@Autowired RabbitTemplate template) throws InterruptedException {
		template.convertAndSend("test.stream.queue2", "foo");
		assertThat(this.config.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.receivedNative).isNotNull();
		assertThat(this.config.context).isNotNull();
		assertThat(this.config.latch3.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.config.latch4.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	@Disabled("Temporary until SF uses Micrometer snaps")
	void queueOverAmqp() throws Exception {
		Client client = new Client("http://guest:guest@localhost:" + managementPort() + "/api");
		QueueInfo queue = client.getQueue("/", "stream.created.over.amqp");
		assertThat(queue.getArguments().get("x-queue-type")).isEqualTo("stream");
	}

	@Configuration(proxyBeanMethods = false)
	@EnableRabbit
	public static class Config {

		final CountDownLatch latch1 = new CountDownLatch(5);

		final CountDownLatch latch2 = new CountDownLatch(1);

		final CountDownLatch latch3 = new CountDownLatch(3);

		final CountDownLatch latch4 = new CountDownLatch(1);

		final List<String> received = new ArrayList<>();

		final AtomicBoolean first = new AtomicBoolean(true);

		volatile Message receivedNative;

		volatile Context context;

		volatile String id;

		@Bean
		static Environment environment() {
			return Environment.builder()
					.addressResolver(add -> new Address("localhost", streamPort()))
					.build();
		}

		@Bean
		SmartLifecycle creator(Environment env) {
			return new SmartLifecycle() {

				boolean running;

				@Override
				public void stop() {
					clean(env);
					this.running = false;
				}

				@Override
				public void start() {
					clean(env);
					env.streamCreator().stream("test.stream.queue1").create();
					env.streamCreator().stream("test.stream.queue2").create();
					this.running = true;
				}

				private void clean(Environment env) {
					try {
						env.deleteStream("test.stream.queue1");
					}
					catch (Exception e) {
					}
					try {
						env.deleteStream("test.stream.queue2");
					}
					catch (Exception e) {
					}
					try {
						env.deleteStream("stream.created.over.amqp");
					}
					catch (Exception e) {
					}
				}

				@Override
				public boolean isRunning() {
					return this.running;
				}

				@Override
				public int getPhase() {
					return 0;
				}


			};
		}

		@Bean
		RabbitListenerContainerFactory<StreamListenerContainer> rabbitListenerContainerFactory(Environment env) {
			StreamRabbitListenerContainerFactory factory = new StreamRabbitListenerContainerFactory(env);
			factory.setAdviceChain(RetryInterceptorBuilder.stateless().build());
			return factory;
		}

		@RabbitListener(queues = "test.stream.queue1")
		void listen(String in) {
			this.received.add(in);
			this.latch1.countDown();
			if (first.getAndSet(false)) {
				throw new RuntimeException("fail first");
			}
		}

		@Bean
		public StreamRetryOperationsInterceptorFactoryBean sfb() {
			StreamRetryOperationsInterceptorFactoryBean rfb = new StreamRetryOperationsInterceptorFactoryBean();
			rfb.setStreamMessageRecoverer((msg, context, throwable) -> {
				this.latch4.countDown();
			});
			return rfb;
		}

		@Bean
		@DependsOn("sfb")
		RabbitListenerContainerFactory<StreamListenerContainer> nativeFactory(Environment env,
				RetryOperationsInterceptor retry) {

			StreamRabbitListenerContainerFactory factory = new StreamRabbitListenerContainerFactory(env);
			factory.setNativeListener(true);
			factory.setConsumerCustomizer((id, builder) -> {
				builder.name(id)
						.offset(OffsetSpecification.first())
						.manualTrackingStrategy();
				if (id.equals("testNative")) {
					this.id = id;
				}
			});
			factory.setAdviceChain(retry);
			return factory;
		}

		@RabbitListener(id = "testNative", queues = "test.stream.queue2", containerFactory = "nativeFactory")
		void nativeMsg(Message in, Context context) {
			this.receivedNative = in;
			this.context = context;
			this.latch2.countDown();
			context.storeOffset();
		}

		@RabbitListener(id = "testNativeFail", queues = "test.stream.queue2", containerFactory = "nativeFactory")
		void nativeMsgFail(Message in, Context context) {
			this.latch3.countDown();
			throw new RuntimeException("fail all");
		}

		@Bean
		CachingConnectionFactory cf() {
			return new CachingConnectionFactory("localhost", amqpPort());
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
