/*
 * Copyright 2002-2016 the original author or authors.
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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.springframework.amqp.utils.SerializationUtils;

/**
 * The 0-8 and 0-9-1 AMQP specifications do not define an Message class or interface. Instead, when performing an
 * operation such as basicPublish the content is passed as a byte-array argument and additional properties are passed in
 * as separate arguments. Spring AMQP defines a Message class as part of a more general AMQP domain model
 * representation. The purpose of the Message class is to simply encapsulate the body and properties within a single
 * instance so that the rest of the AMQP API can in turn be simpler.
 *
 * @author Mark Pollack
 * @author Mark Fisher
 * @author Oleg Zhurakousky
 * @author Dave Syer
 * @author Gary Russell
 */
public class Message implements Serializable {

	private static final long serialVersionUID = -7177590352110605597L;

	private static final String ENCODING = Charset.defaultCharset().name();

	private final MessageProperties messageProperties;

	private final byte[] body;

	public Message(byte[] body, MessageProperties messageProperties) {//NOSONAR
		this.body = body;//NOSONAR
		this.messageProperties = messageProperties;
	}

	public byte[] getBody() {
		return this.body;//NOSONAR
	}

	public MessageProperties getMessageProperties() {
		return this.messageProperties;
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append("(");
		buffer.append("Body:'" + this.getBodyContentAsString() + "'");
		if (this.messageProperties != null) {
			buffer.append(" ").append(this.messageProperties.toString());
		}
		buffer.append(")");
		return buffer.toString();
	}

	private String getBodyContentAsString() {
		if (this.body == null) {
			return null;
		}
		try {
			String contentType = (this.messageProperties != null) ? this.messageProperties.getContentType() : null;
			if (MessageProperties.CONTENT_TYPE_SERIALIZED_OBJECT.equals(contentType)) {
				return SerializationUtils.deserialize(this.body).toString();
			}
			if (MessageProperties.CONTENT_TYPE_TEXT_PLAIN.equals(contentType)
					|| MessageProperties.CONTENT_TYPE_JSON.equals(contentType)
					|| MessageProperties.CONTENT_TYPE_JSON_ALT.equals(contentType)
					|| MessageProperties.CONTENT_TYPE_XML.equals(contentType)) {
				return new String(this.body, ENCODING);
			}
		}
		catch (Exception e) {
			// ignore
		}
		// Comes out as '[B@....b' (so harmless)
		return this.body.toString()+"(byte["+this.body.length+"])";//NOSONAR
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(this.body);
		result = prime * result + ((this.messageProperties == null) ? 0 : this.messageProperties.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Message other = (Message) obj;
		if (!Arrays.equals(this.body, other.body)) {
			return false;
		}
		if (this.messageProperties == null) {
			if (other.messageProperties != null) {
				return false;
			}
		}
		else if (!this.messageProperties.equals(other.messageProperties)) {
			return false;
		}
		return true;
	}


}
