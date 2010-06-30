/*
 * Copyright 2002-2010 the original author or authors.
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

import org.springframework.amqp.utils.SerializationUtils;

/**
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 */
public class Message {

	private final MessageProperties messageProperties;

	private final byte[] body;


	public Message(byte[] body, MessageProperties messageProperties) {
		this.body = body;
		this.messageProperties = messageProperties;
	}


	public byte[] getBody() {
		return this.body;
	}

	public MessageProperties getMessageProperties() {
		return this.messageProperties;
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("(");
		buffer.append("Body:'" + this.getBodyContentAsString() + "';");
		buffer.append("ID:" + messageProperties.getMessageId() + ";");	
		buffer.append("Content:" + messageProperties.getContentType() + ";");
		buffer.append("Headers:" + messageProperties.getHeaders() + ";");
		buffer.append("Exchange:" + messageProperties.getReceivedExchange() + ";");
		buffer.append("Routing key:" + messageProperties.getReceivedRoutingKey() + ";");
		buffer.append("Reply:" + messageProperties.getReplyTo() + ";");
		buffer.append("Delivery mode:" + messageProperties.getDeliveryMode() + ";");
		buffer.append(")");
		return buffer.toString();
	}

	private String getBodyContentAsString() {
		String contentType = messageProperties.getContentType();
		if (contentType.equals(MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT)) {
			return (String) SerializationUtils.deserialize(body);
		}
		else if (contentType.equals(MessageProperties.CONTENT_TYPE_BYTES)) {
			return body.toString();
		}
		else if (contentType.equals(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)) {
			return new String(body);
		}
		else if (contentType.equals(MessageProperties.CONTENT_TYPE_JSON)) {
			//TODO provide JSON conversion to string
			return body.toString();
		}
		return null;
	}

}
