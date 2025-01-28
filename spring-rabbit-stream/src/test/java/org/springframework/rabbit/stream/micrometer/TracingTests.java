/*
 * Copyright 2023-2025 the original author or authors.
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

package org.springframework.rabbit.stream.micrometer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span.Kind;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.testcontainers.junit.jupiter.Testcontainers;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests;
import org.springframework.amqp.rabbit.listener.RabbitListenerContainerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.config.StreamRabbitListenerContainerFactory;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import org.springframework.rabbit.stream.support.StreamAdmin;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Gary Russell
 * @author Artem Bilan
 * @since 3.0.5
 *
 */
@Testcontainers(disabledWithoutDocker = true)
public class TracingTests extends SampleTestRunner {

	private static final AbstractTestContainerTests atct = new AbstractTestContainerTests() {
	};

	@Override
	public SampleTestRunnerConsumer yourCode() throws Exception {
		return (bb, meterRegistry) -> {
			ObservationRegistry observationRegistry = getObservationRegistry();
			try (AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext()) {
				applicationContext.getBeanFactory().registerSingleton("obsReg", observationRegistry);
				applicationContext.register(Config.class);
				applicationContext.refresh();
				applicationContext.getBean(RabbitStreamTemplate.class).convertAndSend("test").get(10, TimeUnit.SECONDS);
				assertThat(applicationContext.getBean(Listener.class).latch1.await(10, TimeUnit.SECONDS)).isTrue();
			}

			List<FinishedSpan> finishedSpans = bb.getFinishedSpans();
			SpansAssert.assertThat(finishedSpans)
					.haveSameTraceId()
					.hasSize(3);
			List<FinishedSpan> producerSpans = finishedSpans.stream()
					.filter(span -> span.getKind().equals(Kind.PRODUCER))
					.collect(Collectors.toList());
			List<FinishedSpan> consumerSpans = finishedSpans.stream()
					.filter(span -> span.getKind().equals(Kind.CONSUMER))
					.collect(Collectors.toList());
			SpanAssert.assertThat(producerSpans.get(0))
					.hasTag("spring.rabbit.stream.template.name", "streamTemplate1");
			SpanAssert.assertThat(producerSpans.get(0))
					.hasRemoteServiceNameEqualTo("RabbitMQ Stream");
			SpanAssert.assertThat(consumerSpans.get(0))
					.hasTagWithKey("spring.rabbit.stream.listener.id");
			SpanAssert.assertThat(consumerSpans.get(0))
					.hasRemoteServiceNameEqualTo("RabbitMQ Stream");
			assertThat(consumerSpans.get(0).getTags().get("spring.rabbit.stream.listener.id")).isIn("one", "two");
			SpanAssert.assertThat(consumerSpans.get(1))
					.hasTagWithKey("spring.rabbit.stream.listener.id");
			assertThat(consumerSpans.get(1).getTags().get("spring.rabbit.stream.listener.id")).isIn("one", "two");
			assertThat(consumerSpans.get(0).getTags().get("spring.rabbit.stream.listener.id"))
					.isNotEqualTo(consumerSpans.get(1).getTags().get("spring.rabbit.stream.listener.id"));
		};
	}

	@EnableRabbit
	@Configuration(proxyBeanMethods = false)
	public static class Config {

		@Bean
		static Environment environment() {
			return Environment.builder()
					.port(AbstractTestContainerTests.streamPort())
					.build();
		}

		@Bean
		StreamAdmin streamAdmin(Environment env) {
			StreamAdmin streamAdmin = new StreamAdmin(env, sc -> {
				sc.stream("trace.stream.queue1").create();
			});
			streamAdmin.setAutoStartup(false);
			return streamAdmin;
		}

		@Bean
		SmartLifecycle creator(Environment env, StreamAdmin admin) {
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
					admin.start();
					this.running = true;
				}

				private void clean(Environment env) {
					try {
						env.deleteStream("trace.stream.queue1");
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
			factory.setObservationEnabled(true);
			factory.setConsumerCustomizer((id, builder) -> {
				builder.name(id)
						.offset(OffsetSpecification.first())
						.manualTrackingStrategy();
			});
			return factory;
		}

		@Bean
		RabbitListenerContainerFactory<StreamListenerContainer> nativeFactory(Environment env) {
			StreamRabbitListenerContainerFactory factory = new StreamRabbitListenerContainerFactory(env);
			factory.setNativeListener(true);
			factory.setObservationEnabled(true);
			factory.setConsumerCustomizer((id, builder) -> {
				builder.name(id)
						.offset(OffsetSpecification.first())
						.manualTrackingStrategy();
			});
			return factory;
		}

		@Bean
		RabbitStreamTemplate streamTemplate1(Environment env) {
			RabbitStreamTemplate template = new RabbitStreamTemplate(env, "trace.stream.queue1");
			template.setProducerCustomizer((name, builder) -> builder.name("test"));
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		Listener listener() {
			return new Listener();
		}

	}

	public static class Listener {

		CountDownLatch latch1 = new CountDownLatch(2);

		@RabbitListener(id = "one", queues = "trace.stream.queue1")
		void listen(String in) {
			latch1.countDown();
		}

		@RabbitListener(id = "two", queues = "trace.stream.queue1", containerFactory = "nativeFactory")
		public void listen(Message in) {
			latch1.countDown();
		}

	}

}
