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

package org.springframework.amqp.rabbit.support.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.test.simple.SimpleSpan;
import io.micrometer.tracing.test.simple.SimpleTracer;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = { "observation.testQ1", "observation.testQ2" })
public class ObservationTests {

	@Test
	void endToEnd(@Autowired Listener listener, @Autowired RabbitTemplate template,
			@Autowired SimpleTracer tracer, @Autowired RabbitListenerEndpointRegistry rler,
			@Autowired MeterRegistry meterRegistry)
					throws InterruptedException {

		template.convertAndSend("observation.testQ1", "test");
		assertThat(listener.latch1.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.message)
				.extracting(msg -> msg.getMessageProperties().getHeaders())
				.hasFieldOrPropertyWithValue("foo", "some foo value")
				.hasFieldOrPropertyWithValue("bar", "some bar value");
		Deque<SimpleSpan> spans = tracer.getSpans();
		assertThat(spans).hasSize(4);
		SimpleSpan span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.rabbit.template.name", "template");
		assertThat(span.getName()).isEqualTo("/observation.testQ1 send");
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.rabbit.listener.id", "obs1", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getName()).isEqualTo("observation.testQ1 receive");
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.rabbit.template.name", "template");
		assertThat(span.getName()).isEqualTo("/observation.testQ2 send");
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.rabbit.listener.id", "obs2", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getName()).isEqualTo("observation.testQ2 receive");
		template.setObservationConvention(new DefaultRabbitTemplateObservationConvention() {

			@Override
			public KeyValues getLowCardinalityKeyValues(RabbitMessageSenderContext context) {
				return super.getLowCardinalityKeyValues(context).and("foo", "bar");
			}

		});
		((AbstractMessageListenerContainer) rler.getListenerContainer("obs1")).setObservationConvention(
				new DefaultRabbitListenerObservationConvention() {

					@Override
					public KeyValues getLowCardinalityKeyValues(RabbitMessageReceiverContext context) {
						return super.getLowCardinalityKeyValues(context).and("baz", "qux");
					}

				});
		rler.getListenerContainer("obs1").stop();
		rler.getListenerContainer("obs1").start();
		template.convertAndSend("observation.testQ1", "test");
		assertThat(listener.latch2.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(listener.message)
				.extracting(msg -> msg.getMessageProperties().getHeaders())
				.hasFieldOrPropertyWithValue("foo", "some foo value")
				.hasFieldOrPropertyWithValue("bar", "some bar value");
		assertThat(spans).hasSize(4);
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.rabbit.template.name", "template");
		assertThat(span.getTags()).containsEntry("foo", "bar");
		assertThat(span.getName()).isEqualTo("/observation.testQ1 send");
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(Map.of("spring.rabbit.listener.id", "obs1", "foo", "some foo value", "bar",
						"some bar value", "baz", "qux"));
		assertThat(span.getName()).isEqualTo("observation.testQ1 receive");
		span = spans.poll();
		assertThat(span.getTags()).containsEntry("spring.rabbit.template.name", "template");
		assertThat(span.getTags()).containsEntry("foo", "bar");
		assertThat(span.getName()).isEqualTo("/observation.testQ2 send");
		span = spans.poll();
		assertThat(span.getTags())
				.containsAllEntriesOf(
						Map.of("spring.rabbit.listener.id", "obs2", "foo", "some foo value", "bar", "some bar value"));
		assertThat(span.getTags()).doesNotContainEntry("baz", "qux");
		assertThat(span.getName()).isEqualTo("observation.testQ2 receive");
		MeterRegistryAssert.assertThat(meterRegistry)
				.hasTimerWithNameAndTags("spring.rabbit.template",
						KeyValues.of("spring.rabbit.template.name", "template"))
				.hasTimerWithNameAndTags("spring.rabbit.template",
						KeyValues.of("spring.rabbit.template.name", "template", "foo", "bar"))
				.hasTimerWithNameAndTags("spring.rabbit.listener", KeyValues.of("spring.rabbit.listener.id", "obs1"))
				.hasTimerWithNameAndTags("spring.rabbit.listener",
						KeyValues.of("spring.rabbit.listener.id", "obs1", "baz", "qux"))
				.hasTimerWithNameAndTags("spring.rabbit.listener", KeyValues.of("spring.rabbit.listener.id", "obs2"));
	}

	@Configuration
	@EnableRabbit
	public static class Config {

		@Bean
		CachingConnectionFactory ccf() {
			return new CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().getConnectionFactory());
		}

		@Bean
		RabbitTemplate template(CachingConnectionFactory ccf) {
			RabbitTemplate template = new RabbitTemplate(ccf);
			template.setObservationEnabled(true);
			return template;
		}

		@Bean
		SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(CachingConnectionFactory ccf) {
			SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
			factory.setConnectionFactory(ccf);
			factory.setContainerCustomizer(container -> container.setObservationEnabled(true));
			return factory;
		}

		@Bean
		SimpleTracer simpleTracer() {
			return new SimpleTracer();
		}

		@Bean
		MeterRegistry meterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		ObservationRegistry observationRegistry(Tracer tracer, Propagator propagator, MeterRegistry meterRegistry) {
			TestObservationRegistry observationRegistry = TestObservationRegistry.create();
			observationRegistry.observationConfig().observationHandler(
					// Composite will pick the first matching handler
					new ObservationHandler.FirstMatchingCompositeObservationHandler(
							// This is responsible for creating a child span on the sender side
							new PropagatingSenderTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a span on the receiver side
							new PropagatingReceiverTracingObservationHandler<>(tracer, propagator),
							// This is responsible for creating a default span
							new DefaultTracingObservationHandler(tracer)))
					.observationHandler(new DefaultMeterObservationHandler(meterRegistry));
			return observationRegistry;
		}

		@Bean
		Propagator propagator(Tracer tracer) {
			return new Propagator() {

				// List of headers required for tracing propagation
				@Override
				public List<String> fields() {
					return Arrays.asList("foo", "bar");
				}

				// This is called on the producer side when the message is being sent
				// Normally we would pass information from tracing context - for tests we don't need to
				@Override
				public <C> void inject(TraceContext context, @Nullable C carrier, Setter<C> setter) {
					setter.set(carrier, "foo", "some foo value");
					setter.set(carrier, "bar", "some bar value");
				}

				// This is called on the consumer side when the message is consumed
				// Normally we would use tools like Extractor from tracing but for tests we are just manually creating a span
				@Override
				public <C> Span.Builder extract(C carrier, Getter<C> getter) {
					String foo = getter.get(carrier, "foo");
					String bar = getter.get(carrier, "bar");
					return tracer.spanBuilder().tag("foo", foo).tag("bar", bar);
				}
			};
		}

		@Bean
		Listener listener(RabbitTemplate template) {
			return new Listener(template);
		}

	}

	public static class Listener {

		private final RabbitTemplate template;

		final CountDownLatch latch1 = new CountDownLatch(1);

		final CountDownLatch latch2 = new CountDownLatch(2);

		volatile Message message;

		public Listener(RabbitTemplate template) {
			this.template = template;
		}

		@RabbitListener(id = "obs1", queues = "observation.testQ1")
		void listen1(Message in) {
			this.template.send("observation.testQ2", in);
		}

		@RabbitListener(id = "obs2", queues = "observation.testQ2")
		void listen2(Message in) {
			this.message = in;
			this.latch1.countDown();
			this.latch2.countDown();
		}

	}

}
