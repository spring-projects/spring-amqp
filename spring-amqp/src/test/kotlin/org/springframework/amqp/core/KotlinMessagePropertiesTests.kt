/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

package org.springframework.amqp.core

import assertk.assertThat
import assertk.assertions.isEqualTo
import assertk.assertions.isNotNull
import assertk.assertions.isTrue
import org.junit.jupiter.api.Test
import java.lang.reflect.Type
import java.util.Date

/**
 * @author Ngoc Nhan
 */
class KotlinMessagePropertiesTests {

	@Test
	fun testSetterForNullabilityFields () {
		val currentDate = Date()
		val messageProperties = MessageProperties()
			messageProperties.amqpAcknowledgment = AmqpAcknowledgmentImpl()
			messageProperties.appId = "appId"
			messageProperties.clusterId = "clusterId"
			messageProperties.consumerQueue = "consumerQueue"
			messageProperties.consumerTag = "consumerTag"
			messageProperties.expiration = "expiration"
			messageProperties.inferredArgumentType = TypeImpl()
			messageProperties.messageCount = 1
			messageProperties.messageId = "messageId"
			messageProperties.receivedDelayLong = 1
			messageProperties.receivedDeliveryMode = MessageDeliveryMode.PERSISTENT
			messageProperties.receivedExchange = "receivedExchange"
			messageProperties.receivedRoutingKey = "receivedRoutingKey"
			messageProperties.receivedUserId = "receivedUserId"
			messageProperties.redelivered = true
			messageProperties.timestamp = currentDate
			messageProperties.type = "type"
			messageProperties.userId = "userId"

		assertThat(messageProperties.amqpAcknowledgment).isEqualTo(AmqpAcknowledgmentImpl())
		assertThat(messageProperties.appId).isEqualTo("appId")
		assertThat(messageProperties.clusterId).isEqualTo("clusterId")
		assertThat(messageProperties.consumerQueue).isEqualTo("consumerQueue")
		assertThat(messageProperties.consumerTag).isEqualTo("consumerTag")
		assertThat(messageProperties.expiration).isEqualTo("expiration")
		assertThat(messageProperties.inferredArgumentType).isEqualTo(TypeImpl())
		assertThat(messageProperties.messageCount).isEqualTo(1)
		assertThat(messageProperties.messageId).isEqualTo("messageId")
		assertThat(messageProperties.receivedDelayLong).isEqualTo(1)
		assertThat(messageProperties.receivedDeliveryMode).isEqualTo(MessageDeliveryMode.PERSISTENT)
		assertThat(messageProperties.receivedExchange).isEqualTo("receivedExchange")
		assertThat(messageProperties.receivedRoutingKey).isEqualTo("receivedRoutingKey")
		assertThat(messageProperties.receivedUserId).isEqualTo("receivedUserId")
		assertThat(messageProperties.redelivered).isNotNull().isTrue()
		assertThat(messageProperties.timestamp).isEqualTo(currentDate)
		assertThat(messageProperties.type).isEqualTo("type")
		assertThat(messageProperties.userId).isEqualTo("userId")
	}

	class AmqpAcknowledgmentImpl: AmqpAcknowledgment {

		override fun acknowledge(status: AmqpAcknowledgment.Status) {
			TODO("Not yet implemented")
		}

		override fun equals(other: Any?): Boolean {
			if (this === other) return true
			if (other !is AmqpAcknowledgmentImpl) return false

			return true
		}

	}

	class TypeImpl: Type {

		override fun equals(other: Any?): Boolean {
			if (this === other) return true
			if (other !is TypeImpl) return false

			return true
		}

	}

}
