/*
 * Copyright 2018 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation

import assertk.assert
import assertk.assertions.isTrue
import org.junit.jupiter.api.Test
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.junit.RabbitAvailable
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Kotlin Annotated listener tests.
 *
 * @author Gary Russell
 *
 * @since 2.1
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = ["kotlinQueue", "kotlinQueue1"])
@DirtiesContext
class EnableRabbitKotlinTests {

	@Autowired
	private lateinit var config: Config

	@Test
	fun `send and wait for consume` () {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinQueue", "test")
		assert(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	fun `send and wait for consume with EH` () {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinQueue1", "test")
		assert(this.config.ehLatch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Configuration
	@EnableRabbit
	class Config {

		val latch = CountDownLatch(1)

		@RabbitListener(queues = ["kotlinQueue"])
		fun handle(@Suppress("UNUSED_PARAMETER") data: String) {
			this.latch.countDown()
		}

		@Bean
		fun rabbitListenerContainerFactory(cf: CachingConnectionFactory): SimpleRabbitListenerContainerFactory {
			val factory = SimpleRabbitListenerContainerFactory()
			factory.setConnectionFactory(cf)
			return factory
		}

		@Bean
		fun cf(): CachingConnectionFactory {
			return CachingConnectionFactory(
					RabbitAvailableCondition.getBrokerRunning().connectionFactory)
		}

		@Bean
		fun multi(): Multi {
			return Multi()
		}

		val ehLatch = CountDownLatch(1)

		@Bean
		fun eh() = RabbitListenerErrorHandler { _, _, _ ->
			this.ehLatch.countDown()
			null
		}

	}

	@RabbitListener(queues = ["kotlinQueue1"], errorHandler = "#{eh}")
	class Multi {

		@RabbitHandler
		fun handle(data: String) {
			throw RuntimeException("fail")
		}

	}

}
