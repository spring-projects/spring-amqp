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

package org.springframework.amqp.support.converter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.TestMessageProperties;
import org.springframework.oxm.Marshaller;
import org.springframework.oxm.Unmarshaller;
import org.springframework.oxm.XmlMappingException;

/**
 * @author Mark Fisher
 */
public class MarshallingMessageConverterTests {

	@Test
	public void marshal() throws Exception {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.afterPropertiesSet();
		Message message = converter.toMessage("marshal test", new TestMessageProperties());
		String response = new String(message.getBody(), "UTF-8");
		assertEquals("MARSHAL TEST", response);
	}

	@Test
	public void unmarshal() {
		TestMarshaller marshaller = new TestMarshaller();
		MarshallingMessageConverter converter = new MarshallingMessageConverter(marshaller);
		converter.afterPropertiesSet();
		Message message = new Message("UNMARSHAL TEST".getBytes(), new TestMessageProperties());
		Object result = converter.fromMessage(message);
		assertEquals("unmarshal test", result);
	}


	private static class TestMarshaller implements Marshaller, Unmarshaller {

		public boolean supports(Class<?> clazz) {
			return true;
		}

		public void marshal(Object graph, Result result) throws IOException, XmlMappingException {
			String response = ((String) graph).toUpperCase();
			((StreamResult) result).getOutputStream().write(response.getBytes());
		}

		public Object unmarshal(Source source) throws IOException, XmlMappingException {
			byte[] buffer = new byte["UNMARSHAL TEST".length()];
			((StreamSource) source).getInputStream().read(buffer);
			return new String(buffer, "UTF-8").toLowerCase();
		}
	}

}
