/*
 * Copyright 2002-2014 the original author or authors.
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

package org.springframework.amqp.support.converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.XmlMappingException;
import org.springframework.util.Assert;

/**
 * Spring Rabbit {@link MessageConverter} that uses a {@link Marshaller} and {@link Unmarshaller}.
 * Marshals an object to a {@link Message} and unmarshals a {@link Message} to an object.
 *
 * @author Mark Fisher
 * @author Arjen Poutsma
 * @author Juergen Hoeller
 * @author James Carr
 * @see org.springframework.amqp.core.AmqpTemplate#convertAndSend(Object)
 * @see org.springframework.amqp.core.AmqpTemplate#receiveAndConvert()
 */
public class MarshallingMessageConverter extends AbstractMessageConverter implements InitializingBean {
	private volatile Marshaller marshaller;

	private volatile Unmarshaller unmarshaller;

	private volatile String contentType;


	/**
	 * Construct a new <code>MarshallingMessageConverter</code> with no {@link Marshaller} or {@link Unmarshaller} set.
	 * The marshaller must be set after construction by invoking {@link #setMarshaller(Marshaller)} and
	 * {@link #setUnmarshaller(Unmarshaller)} .
	 */
	public MarshallingMessageConverter() {
	}

	/**
	 * Construct a new <code>MarshallingMessageConverter</code> with the given {@link Marshaller} set.
	 * <p>If the given {@link Marshaller} also implements the {@link Unmarshaller} interface,
	 * it is used for both marshalling and unmarshalling. Otherwise, an exception is thrown.
	 * <p>Note that all {@link Marshaller} implementations in Spring also implement the
	 * {@link Unmarshaller} interface, so that you can safely use this constructor.
	 * @param marshaller object used as marshaller and unmarshaller
	 * @throws IllegalArgumentException when <code>marshaller</code> does not implement the
	 * {@link Unmarshaller} interface as well
	 */
	public MarshallingMessageConverter(Marshaller marshaller) {
		Assert.notNull(marshaller, "marshaller must not be null");
		if (!(marshaller instanceof Unmarshaller)) {
			throw new IllegalArgumentException(
					"Marshaller [" + marshaller + "] does not implement the Unmarshaller " +
					"interface. Please set an Unmarshaller explicitly by using the " +
					"MarshallingMessageConverter(Marshaller, Unmarshaller) constructor.");
		}
		else {
			this.marshaller = marshaller;
			this.unmarshaller = (Unmarshaller) marshaller;
		}
	}

	/**
	 * Construct a new <code>MarshallingMessageConverter</code> with the
	 * given Marshaller and Unmarshaller.
	 * @param marshaller the Marshaller to use
	 * @param unmarshaller the Unmarshaller to use
	 */
	public MarshallingMessageConverter(Marshaller marshaller, Unmarshaller unmarshaller) {
		Assert.notNull(marshaller, "marshaller must not be null");
		Assert.notNull(unmarshaller, "unmarshaller must not be null");
		this.marshaller = marshaller;
		this.unmarshaller = unmarshaller;
	}


	/**
	 * Set the contentType to be used by this message converter.
	 *
	 * @param contentType The content type.
	 */
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	/**
	 * Set the {@link Marshaller} to be used by this message converter.
	 *
	 * @param marshaller The marshaller.
	 */
	public void setMarshaller(Marshaller marshaller) {
		Assert.notNull(marshaller, "marshaller must not be null");
		this.marshaller = marshaller;
	}

	/**
	 * Set the {@link Unmarshaller} to be used by this message converter.
	 *
	 * @param unmarshaller The unmarshaller.
	 */
	public void setUnmarshaller(Unmarshaller unmarshaller) {
		Assert.notNull(unmarshaller, "unmarshaller must not be null");
		this.unmarshaller = unmarshaller;
	}

	@Override
	public void afterPropertiesSet() {
		Assert.notNull(this.marshaller, "Property 'marshaller' is required");
		Assert.notNull(this.unmarshaller, "Property 'unmarshaller' is required");
	}


	/**
	 * Marshals the given object to a {@link Message}.
	 */
	@Override
	protected Message createMessage(Object object, MessageProperties messageProperties) throws MessageConversionException {
		try {
			if (this.contentType != null) {
				messageProperties.setContentType(this.contentType);
			}
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			StreamResult streamResult = new StreamResult(bos);
			this.marshaller.marshal(object, streamResult);
			return new Message(bos.toByteArray(), messageProperties);
		}
		catch (XmlMappingException ex) {
			throw new MessageConversionException("Could not marshal [" + object + "]", ex);
		}
		catch (IOException ex) {
			throw new MessageConversionException("Could not marshal  [" + object + "]", ex);
		}
	}

	/**
	 * Unmarshals the given {@link Message} into an object.
	 */
	@Override
	public Object fromMessage(Message message) throws MessageConversionException {
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(message.getBody());
			StreamSource source = new StreamSource(bis);
			return this.unmarshaller.unmarshal(source);
		}
		catch (IOException ex) {
			throw new MessageConversionException("Could not access message content: " + message, ex);
		}
		catch (XmlMappingException ex) {
			throw new MessageConversionException("Could not unmarshal message: " + message, ex);
		}
	}

}
