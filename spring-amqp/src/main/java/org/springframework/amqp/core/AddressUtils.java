/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.amqp.core;

/**
 * @author Gary Russell
 * @since 1.4.1
 *
 */
@Deprecated
public final class AddressUtils {

	/**
	 * @deprecated Use the constant in {@link Address#AMQ_RABBITMQ_REPLY_TO}.
	 */
	@Deprecated
	public static final String AMQ_RABBITMQ_REPLY_TO = Address.AMQ_RABBITMQ_REPLY_TO;

	private AddressUtils() {
		super();
	}

	/**
	 * Decodes the reply-to {@link Address} into exchange/key.
	 *
	 * @param request the inbound message.
	 * @return the Address.
	 */
	public static Address decodeReplyToAddress(Message request) {
		return request.getMessageProperties().getReplyToAddress();
	}

}
