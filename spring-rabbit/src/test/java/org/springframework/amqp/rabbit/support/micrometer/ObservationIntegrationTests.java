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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import io.micrometer.common.KeyValues;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span.Kind;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpanAssert;
import io.micrometer.tracing.test.simple.SpansAssert;

/**
 * @author Artem Bilan
 * @author Gary Russell
 *
 * @since 3.0
 */
@RabbitAvailable(queues = { "int.observation.testQ1", "int.observation.testQ2" })
public class ObservationIntegrationTests extends SampleTestRunner {

	@Override
	public SampleTestRunnerConsumer yourCode() {
		// template -> listener -> template -> listener
		return (bb, meterRegistry) -> {
			ObservationRegistry observationRegistry = getObservationRegistry();
			try (AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext()) {
				applicationContext.registerBean(ObservationRegistry.class, () -> observationRegistry);
				applicationContext.register(Config.class);
				applicationContext.refresh();
				applicationContext.getBean(RabbitTemplate.class).convertAndSend("int.observation.testQ1", "test");
				assertThat(applicationContext.getBean(Listener.class).latch1.await(10, TimeUnit.SECONDS)).isTrue();
			}

			List<FinishedSpan> finishedSpans = bb.getFinishedSpans();
			SpansAssert.assertThat(finishedSpans)
					.haveSameTraceId()
					.hasSize(4);
			SpanAssert.assertThat(finishedSpans.get(0))
					.hasKindEqualTo(Kind.PRODUCER)
					.hasTag("bean.name", "template");
			SpanAssert.assertThat(finishedSpans.get(1))
					.hasKindEqualTo(Kind.PRODUCER)
					.hasTag("bean.name", "template");
			SpanAssert.assertThat(finishedSpans.get(2))
					.hasKindEqualTo(Kind.CONSUMER)
					.hasTag("listener.id", "obs1");
			SpanAssert.assertThat(finishedSpans.get(3))
					.hasKindEqualTo(Kind.CONSUMER)
					.hasTag("listener.id", "obs2");

			MeterRegistryAssert.assertThat(getMeterRegistry())
					.hasTimerWithNameAndTags("spring.rabbit.template", KeyValues.of("bean.name", "template"))
					.hasTimerWithNameAndTags("spring.rabbit.template", KeyValues.of("bean.name", "template"))
					.hasTimerWithNameAndTags("spring.rabbit.listener", KeyValues.of("listener.id", "obs1"))
					.hasTimerWithNameAndTags("spring.rabbit.listener", KeyValues.of("listener.id", "obs2"));
		};
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
		Listener listener(RabbitTemplate template) {
			return new Listener(template);
		}

	}

	public static class Listener {

		private final RabbitTemplate template;

		final CountDownLatch latch1 = new CountDownLatch(1);

		volatile Message message;

		public Listener(RabbitTemplate template) {
			this.template = template;
		}

		@RabbitListener(id = "obs1", queues = "int.observation.testQ1")
		void listen1(Message in) {
			this.template.convertAndSend("int.observation.testQ2", in);
		}

		@RabbitListener(id = "obs2", queues = "int.observation.testQ2")
		void listen2(Message in) {
			this.message = in;
			this.latch1.countDown();
		}

	}


}
