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

package org.springframework.amqp.rabbit.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * @author Gary Russell
 */
public class MicrometerHolderTests {

	@Test
	void multiReg() {
		ApplicationContext context = new AnnotationConfigApplicationContext(Config1.class);
		assertThatIllegalStateException().isThrownBy(() -> new MicrometerHolder(context, "", Collections.emptyMap()))
				.withMessage("No micrometer registry present (or more than one and "
						+ "there is not exactly one marked with @Primary)");
	}

	@Test
	void twoPrimaries() {
		ApplicationContext context = new AnnotationConfigApplicationContext(Config2.class);
		assertThatIllegalStateException().isThrownBy(() -> new MicrometerHolder(context, "", Collections.emptyMap()))
				.withMessageContaining("more than one 'primary' bean");
	}

	@Test
	void primary() {
		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext(Config3.class);
		MicrometerHolder micrometerHolder = new MicrometerHolder(ctx, "holderName", Collections.emptyMap());
		Timer.Sample sample = mock(Timer.Sample.class);
		micrometerHolder.success(sample, "queue");
		micrometerHolder.failure(sample, "queue", "SomeException");
		@SuppressWarnings("unchecked")
		Map<String, Timer> meters = (Map<String, Timer>) ReflectionTestUtils.getField(micrometerHolder, "timers");
		assertThat(meters).hasSize(2);
		ctx.close();
		micrometerHolder.destroy();
		assertThat(meters).hasSize(0);
	}

	static class Config1 {

		@Bean
		MeterRegistry reg1() {
			return new SimpleMeterRegistry();
		}

		@Bean
		MeterRegistry reg2() {
			return new SimpleMeterRegistry();
		}

	}

	static class Config2 {

		@Bean
		@Primary
		MeterRegistry reg1() {
			return new SimpleMeterRegistry();
		}

		@Bean
		@Primary
		MeterRegistry reg2() {
			return new SimpleMeterRegistry();
		}

	}

	static class Config3 {

		@Bean
		@Primary
		MeterRegistry reg1() {
			return new SimpleMeterRegistry();
		}

		@Bean
		MeterRegistry reg2() {
			return new SimpleMeterRegistry();
		}

	}

}
