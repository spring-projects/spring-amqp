/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.amqp.rabbit.annotation

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isTrue
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.junit.RabbitAvailable
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler
import org.springframework.aop.framework.ProxyFactory
import org.springframework.beans.BeansException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.BeanPostProcessor
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Kotlin Annotated listener tests.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.1
 *
 */
@SpringJUnitConfig
@RabbitAvailable(queues = ["kotlinQueue", "kotlinQueue1", "kotlinReplyQueue"])
@DirtiesContext
class EnableRabbitKotlinTests {

	@Autowired
	private lateinit var config: Config

	@Test
	fun `send and wait for consume`() {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinQueue", "test")
		assertThat(this.config.latch.await(10, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	fun `send and wait for consume with EH`() {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinQueue1", "test")
		assertThat(this.config.ehLatch.await(10, TimeUnit.SECONDS)).isTrue();
		val reply = template.receiveAndConvert("kotlinReplyQueue", 10_000)
		assertThat(reply).isEqualTo("error processed");
	}

	@Configuration
	@EnableRabbit
	class Config {

		val latch = CountDownLatch(1)

		@RabbitListener(queues = ["kotlinQueue"])
		suspend fun handle(@Suppress("UNUSED_PARAMETER") data: String) {
			this.latch.countDown()
		}

		@Bean
		fun rabbitListenerContainerFactory(cf: CachingConnectionFactory) =
				SimpleRabbitListenerContainerFactory().also {
					it.setAcknowledgeMode(AcknowledgeMode.MANUAL)
					it.setConnectionFactory(cf)
				}

		@Bean
		fun cf() = CachingConnectionFactory(RabbitAvailableCondition.getBrokerRunning().connectionFactory)

		@Bean
		fun multi() = Multi()

		@Bean
		fun proxyListenerPostProcessor(): BeanPostProcessor? {
			return object : BeanPostProcessor {
				@Throws(BeansException::class)
				override fun postProcessBeforeInitialization(bean: Any, beanName: String): Any {
					if (bean is Multi) {
						val proxyFactory = ProxyFactory(bean)
						proxyFactory.isProxyTargetClass = true
						return proxyFactory.proxy
					}
					return bean
				}
			}
		}

		val ehLatch = CountDownLatch(1)

		@Bean
		fun eh() = RabbitListenerErrorHandler { _, _, _ ->
			this.ehLatch.countDown()
			"error processed"
		}

	}

	@RabbitListener(queues = ["kotlinQueue1"], errorHandler = "#{eh}")
	@SendTo("kotlinReplyQueue")
	open class Multi {

		@RabbitHandler
		fun handle(@Suppress("UNUSED_PARAMETER") data: String) {
			throw RuntimeException("fail")
		}

	}

}
