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

package org.springframework.amqp.support.converter;

import java.io.IOException;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.XmlMappingException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Fisher
 * @author James Carr
 */
public class MarshallingMessageConverterTests {
	@Test
	public void marshal() throws Exception {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.afterPropertiesSet();
		Message message = converter.toMessage("marshal test", new MessageProperties());
		String response = new String(message.getBody(), "UTF-8");
		assertThat(response).isEqualTo("MARSHAL TEST");
	}

	@Test
	public void marshalIncludesContentType() throws Exception {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.setContentType(MessageProperties.CONTENT_TYPE_XML);
		converter.afterPropertiesSet();

		Message message = converter.toMessage("marshal test", new MessageProperties());

		assertThat(message.getMessageProperties().getContentType()).isEqualTo("application/xml");
	}

	@Test
	public void dontSetNullContentType() throws Exception {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.afterPropertiesSet();
		final String defaultContentType = new MessageProperties().getContentType();

		Message message = converter.toMessage("marshal test", new MessageProperties());

		assertThat(message.getMessageProperties().getContentType()).isEqualTo(defaultContentType);
	}

	@Test
	public void unmarshal() {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.afterPropertiesSet();
		Message message = new Message("UNMARSHAL TEST".getBytes(), new MessageProperties());
		Object result = converter.fromMessage(message);
		assertThat(result).isEqualTo("unmarshal test");
	}


	private static class TestMarshaller implements Marshaller, Unmarshaller {

		TestMarshaller() {
		}

		@Override
		public boolean supports(Class<?> clazz) {
			return true;
		}

		@Override
		public void marshal(Object graph, Result result) throws IOException, XmlMappingException {
			String response = ((String) graph).toUpperCase();
			((StreamResult) result).getOutputStream().write(response.getBytes());
		}

		@Override
		public Object unmarshal(Source source) throws IOException, XmlMappingException {
			byte[] buffer = new byte["UNMARSHAL TEST".length()];
			((StreamSource) source).getInputStream().read(buffer);
			return new String(buffer, "UTF-8").toLowerCase();
		}
	}

}
