/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.annotation

import assertk.assertThat
import assertk.assertions.isEqualTo
import org.apache.qpid.protonj2.client.Client
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.amqp.client.AmqpClient
import org.springframework.amqp.client.AmqpConnectionFactory
import org.springframework.amqp.client.SingleAmqpConnectionFactory
import org.springframework.amqp.client.config.AmqpDefaultConfiguration
import org.springframework.amqp.client.config.EnableAmqp
import org.springframework.amqp.client.config.MethodAmqpMessageListenerContainerFactory
import org.springframework.amqp.rabbit.junit.AbstractTestContainerTests
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter
import org.springframework.amqp.support.converter.MessageConverter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import java.io.IOException
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * @author Artem Bilan
 *
 * @since 4.1
 */
@SpringJUnitConfig
@DirtiesContext
class KotlinAmqpListenerAnnotationTests : AbstractTestContainerTests() {

	companion object {

		const val TEST_QUEUE1: String = "/queues/kotlin_listener1"

		const val TEST_QUEUE2: String = "/queues/kotlin_listener2"

		const val TEST_REPLY_TO: String = "/queues/kotlin_listener_reply_to"

		@BeforeAll
		@JvmStatic
		@Throws(IOException::class, InterruptedException::class)
		fun initQueues() {
			for (queue in arrayOf(TEST_QUEUE1, TEST_QUEUE2, TEST_REPLY_TO)) {
				RABBITMQ!!.execInContainer(
					"rabbitmqadmin",
					"queues",
					"declare",
					"--name",
					queue.replaceFirst("/queues/".toRegex(), "")
				)
			}
		}
	}

	@Autowired
	lateinit var amqpClient: AmqpClient

	@Autowired
	lateinit var testConfig: TestConfig

	@Test
	fun kotlinSimpleAmqpListener() {
		this.amqpClient.to(TEST_QUEUE1)
			.body("test_data")
			.send()

		val result = this.testConfig.results.poll(10, TimeUnit.SECONDS)

		assertThat(result).isEqualTo("test_data")
	}

	@Test
	fun kotlinRequestReplySuspendAmqpListener() {
		val dataIn = DataIn("test_data")
		this.amqpClient.to(TEST_QUEUE2)
			.body(dataIn)
			.send()

		val dataOut: CompletableFuture<DataOut> =
			this.amqpClient.from(TEST_REPLY_TO)
				.receiveAndConvert()

		assertThat(dataOut)
			.transform { it.get(10, TimeUnit.SECONDS) }
			.isEqualTo(DataOut(dataIn.data + "_out"))
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAmqp
	class TestConfig {

		@Bean
		fun jsonMessageConverter() = JacksonJsonMessageConverter()

		@Bean
		fun amqpConnectionFactory(protonClient: Client) =
			SingleAmqpConnectionFactory(protonClient)
				.setPort(amqpPort())

		@Bean
		fun amqpClient(connectionFactory: AmqpConnectionFactory, jsonMessageConverter: MessageConverter) =
			AmqpClient.builder(connectionFactory)
				.messageConverter(jsonMessageConverter)
				.build()

		@Bean(AmqpDefaultConfiguration.DEFAULT_AMQP_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
		fun containerFactory(connectionFactory: AmqpConnectionFactory, jsonMessageConverter: MessageConverter) =
			MethodAmqpMessageListenerContainerFactory(connectionFactory)
				.also { it.setMessageConverter(jsonMessageConverter) }

		val results: BlockingQueue<Any> = LinkedBlockingQueue<Any>()

		@AmqpListener(addresses = [TEST_QUEUE1])
		fun simpleListener(payload: String) {
			this.results.add(payload)
		}

		@AmqpListener(addresses = [TEST_QUEUE2])
		@SendTo(TEST_REPLY_TO)
		suspend fun requestReplyAsyncListener(payload: DataIn) = DataOut(payload.data + "_out")

	}

	data class DataIn(val data: String)

	data class DataOut(val data: String)

}
