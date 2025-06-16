/*
 * Copyright 2002-present the original author or authors.
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

package org.springframework.amqp.support;

import org.springframework.messaging.MessageHeaders;

/**
 * Pre-defined names and prefixes to be used for setting and/or retrieving AMQP
 * MessageProperties from/to {@link org.springframework.messaging.Message
 * Message} Headers.
 *
 * @author Mark Fisher
 * @author Gary Russell
 * @since 1.4
 */
public abstract class AmqpHeaders {

	/**
	 * Prefix used for AMQP related headers in order to distinguish from
	 * user-defined headers and other internal headers (e.g. replyTo).
	 * @see SimpleAmqpHeaderMapper
	 */
	public static final String PREFIX = "amqp_";


	// Header Name Constants

	public static final String APP_ID = PREFIX + "appId";

	public static final String CLUSTER_ID = PREFIX + "clusterId";

	public static final String CONTENT_ENCODING = PREFIX + "contentEncoding";

	public static final String CONTENT_LENGTH = PREFIX + "contentLength";

	public static final String CONTENT_TYPE = MessageHeaders.CONTENT_TYPE;

	public static final String CONTENT_TYPE_CONVERTER_WINS = PREFIX + "contentTypeConverterWins";

	public static final String CORRELATION_ID = PREFIX + "correlationId";

	public static final String DELAY = PREFIX + "delay";

	public static final String DELIVERY_MODE = PREFIX + "deliveryMode";

	public static final String DELIVERY_TAG = PREFIX + "deliveryTag";

	public static final String EXPIRATION = PREFIX + "expiration";

	public static final String MESSAGE_COUNT = PREFIX + "messageCount";

	public static final String MESSAGE_ID = PREFIX + "messageId";

	public static final String RECEIVED_DELAY = PREFIX + "receivedDelay";

	public static final String RECEIVED_DELIVERY_MODE = PREFIX + "receivedDeliveryMode";

	public static final String RECEIVED_EXCHANGE = PREFIX + "receivedExchange";

	public static final String RECEIVED_ROUTING_KEY = PREFIX + "receivedRoutingKey";

	public static final String RECEIVED_USER_ID = PREFIX + "receivedUserId";

	public static final String REDELIVERED = PREFIX + "redelivered";

	public static final String REPLY_TO = PREFIX + "replyTo";

	public static final String TIMESTAMP = PREFIX + "timestamp";

	public static final String TYPE = PREFIX + "type";

	public static final String USER_ID = PREFIX + "userId";

	public static final String SPRING_REPLY_CORRELATION = PREFIX + "springReplyCorrelation";

	public static final String SPRING_REPLY_TO_STACK = PREFIX + "springReplyToStack";

	/**
	 * A CorrelationData instance for publisher confirms (not mapped).
	 * @since 2.3
	 */
	public static final String PUBLISH_CONFIRM_CORRELATION = PREFIX + "publishConfirmCorrelation";

	public static final String PUBLISH_CONFIRM = PREFIX + "publishConfirm";

	public static final String PUBLISH_CONFIRM_NACK_CAUSE = PREFIX + "publishConfirmNackCause";

	public static final String RETURN_REPLY_CODE = PREFIX + "returnReplyCode";

	public static final String RETURN_REPLY_TEXT = PREFIX + "returnReplyText";

	public static final String RETURN_EXCHANGE = PREFIX + "returnExchange";

	public static final String RETURN_ROUTING_KEY = PREFIX + "returnRoutingKey";

	public static final String CHANNEL = PREFIX + "channel";

	/**
	 * The tag of the listener container consumer that received the message.
	 */
	public static final String CONSUMER_TAG = PREFIX + "consumerTag";

	/**
	 * The queue from which the listener container consumer received the message.
	 */
	public static final String CONSUMER_QUEUE = PREFIX + "consumerQueue";

	/**
	 * The received raw message before conversion; this header usually only appears in
	 * an {@link org.springframework.messaging.support.ErrorMessage}.
	 * @since 2.0
	 */
	public static final String RAW_MESSAGE = PREFIX + "raw_message";

	/**
	 * A flag to indicate that the current message is the last from a batch.
	 * @since 2.2
	 */
	public static final String LAST_IN_BATCH = PREFIX + "lastInBatch";

	/**
	 * The number of fragments in a batch message.
	 * @since 2.2
	 */
	public static final String BATCH_SIZE = PREFIX + "batchSize";

	/**
	 * The number of retries for the message over server republishing.
	 * @since 3.2
	 */
	public static final String RETRY_COUNT = PREFIX + "retryCount";

}
