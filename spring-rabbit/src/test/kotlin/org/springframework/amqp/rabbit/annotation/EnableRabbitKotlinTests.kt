/*
 * Copyright 2018-2024 the original author or authors.
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
import assertk.assertions.containsOnly
import assertk.assertions.isEqualTo
import assertk.assertions.isTrue
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.AcknowledgeMode
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory
import org.springframework.amqp.rabbit.core.RabbitTemplate
import org.springframework.amqp.rabbit.junit.RabbitAvailable
import org.springframework.amqp.rabbit.junit.RabbitAvailableCondition
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler
import org.springframework.amqp.utils.test.TestUtils
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
import java.util.concurrent.atomic.AtomicBoolean

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
@RabbitAvailable(queues = ["kotlinQueue", "kotlinBatchQueue", "kotlinQueue1", "kotlinReplyQueue"])
@DirtiesContext
class EnableRabbitKotlinTests {

	@Autowired
	private lateinit var config: Config

	@Test
	fun `send and wait for consume`(@Autowired registry: RabbitListenerEndpointRegistry) {
		val template = RabbitTemplate(this.config.cf())
		template.setReplyTimeout(10_000)
		val result = template.convertSendAndReceive("kotlinQueue", "test")
		assertThat(result).isEqualTo("TEST")
		val listener = registry.getListenerContainer("single").messageListener
		assertThat(TestUtils.getPropertyValue(listener, "messagingMessageConverter.inferredArgumentType").toString())
				.isEqualTo("class java.lang.String")
	}

	@Test
	fun `listen for batch`() {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinBatchQueue", "test1")
		template.convertAndSend("kotlinBatchQueue", "test2")
		assertThat(this.config.batchReceived.await(10, TimeUnit.SECONDS)).isTrue()
		assertThat(this.config.batch).containsOnly("test1", "test2")
	}

	@Test
	fun `send and wait for consume with EH`() {
		val template = RabbitTemplate(this.config.cf())
		template.convertAndSend("kotlinQueue1", "test")
		assertThat(this.config.ehLatch.await(10, TimeUnit.SECONDS)).isTrue()
		val reply = template.receiveAndConvert("kotlinReplyQueue", 10_000)
		assertThat(reply).isEqualTo("error processed")
	}

	@Configuration
	@EnableRabbit
	class Config {

		@RabbitListener(id = "single", queues = ["kotlinQueue"])
		suspend fun handle(@Suppress("UNUSED_PARAMETER") data: String) : String? {
			return data.uppercase()
		}

		val batchReceived = CountDownLatch(1)

		lateinit var batch: List<String>

		@RabbitListener(id = "batch", queues = ["kotlinBatchQueue"],
				containerFactory = "batchRabbitListenerContainerFactory")
		suspend fun receiveBatch(messages: List<String>) {
			batch = messages
			batchReceived.countDown()
		}

		@Bean
		fun rabbitListenerContainerFactory(cf: CachingConnectionFactory) =
				SimpleRabbitListenerContainerFactory().also {
					it.setAcknowledgeMode(AcknowledgeMode.MANUAL)
					it.setReceiveTimeout(10)
					it.setConnectionFactory(cf)
				}

		@Bean
		fun batchRabbitListenerContainerFactory(cf: CachingConnectionFactory) =
				SimpleRabbitListenerContainerFactory().also {
					it.setAcknowledgeMode(AcknowledgeMode.MANUAL)
					it.setConsumerBatchEnabled(true)
					it.setDeBatchingEnabled(true)
					it.setBatchSize(3)
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
		fun eh() = RabbitListenerErrorHandler { _, _, _, _ ->
			this.ehLatch.countDown()
			"error processed"
		}

	}

	@RabbitListener(id = "multi", queues = ["kotlinQueue1"], errorHandler = "#{eh}")
	@SendTo("kotlinReplyQueue")
	open class Multi {

		@RabbitHandler
		fun handle(@Suppress("UNUSED_PARAMETER") data: String) {
			throw RuntimeException("fail")
		}

	}

}
