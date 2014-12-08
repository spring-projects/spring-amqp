/*
 * Copyright 2014 the original author or authors.
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
public class AddressUtils {

	public static final String AMQ_RABBITMQ_REPLY_TO = "amq.rabbitmq.reply-to";

	/**
	 * Decodes the reply-to {@link Address} into exchange/key.
	 *
	 * @param request the inbound message.
	 * @return the Address.
	 */
	public static Address decodeReplyToAddress(Message request) {
		Address replyTo;
		String replyToString = request.getMessageProperties().getReplyTo();
		if (replyToString == null) {
			replyTo = null;
		}
		else if (replyToString.startsWith(AMQ_RABBITMQ_REPLY_TO)) {
			replyTo = new Address("", replyToString);
		}
		else {
			replyTo = new Address(replyToString);
		}
		return replyTo;
	}

}
