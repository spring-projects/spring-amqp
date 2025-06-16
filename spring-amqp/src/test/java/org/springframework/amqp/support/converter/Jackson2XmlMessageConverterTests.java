/*
 * Copyright 2018-present the original author or authors.
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

import java.math.BigDecimal;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mohammad Hewedy
 * @author Gary Russell
 *
 * @since 2.1
 */
@SpringJUnitConfig
@DirtiesContext
public class Jackson2XmlMessageConverterTests {

	public static final String TRUSTED_PACKAGE = Jackson2XmlMessageConverterTests.class.getPackage().getName();

	private Jackson2XmlMessageConverter converter;

	private SimpleTrade trade;

	@Autowired
	private Jackson2XmlMessageConverter xmlConverterWithDefaultType;

	@BeforeEach
	public void before() {
		converter = new Jackson2XmlMessageConverter(TRUSTED_PACKAGE);
		trade = new SimpleTrade();
		trade.setAccountName("Acct1");
		trade.setBuyRequest(true);
		trade.setOrderType("Market");
		trade.setPrice(new BigDecimal(103.30));
		trade.setQuantity(100);
		trade.setRequestId("R123");
		trade.setTicker("VMW");
		trade.setUserName("Joe Trader");
	}

	@Test
	public void simpleTrade() {
		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(trade).isEqualTo(marshalledTrade);
	}

	@Test
	public void simpleTradeOverrideMapper() {
		XmlMapper mapper = new XmlMapper();
		mapper.setSerializerFactory(BeanSerializerFactory.instance);
		converter = new Jackson2XmlMessageConverter(mapper);

		((DefaultJackson2JavaTypeMapper) this.converter.getJavaTypeMapper())
				.setTrustedPackages(TRUSTED_PACKAGE);

		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(trade).isEqualTo(marshalledTrade);
	}

	@Test
	public void nestedBean() {
		Bar bar = new Bar();
		bar.getFoo().setName("spam");

		Message message = converter.toMessage(bar, new MessageProperties());

		Bar marshalled = (Bar) converter.fromMessage(message);
		assertThat(bar).isEqualTo(marshalled);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void hashtable() {
		Hashtable<String, String> hashtable = new Hashtable<String, String>();
		hashtable.put("TICKER", "VMW");
		hashtable.put("PRICE", "103.2");

		Message message = converter.toMessage(hashtable, new MessageProperties());
		Hashtable<String, String> marhsalledHashtable = (Hashtable<String, String>) converter.fromMessage(message);

		assertThat("VMW").isEqualTo(marhsalledHashtable.get("TICKER"));
		assertThat("103.2").isEqualTo(marhsalledHashtable.get("PRICE"));
	}

	@Test
	public void shouldUseClassMapperWhenProvided() {
		Message message = converter.toMessage(trade, new MessageProperties());

		converter.setClassMapper(new DefaultClassMapper());

		((DefaultClassMapper) this.converter.getClassMapper()).setTrustedPackages(TRUSTED_PACKAGE);

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(trade).isEqualTo(marshalledTrade);
	}

	@Test
	public void shouldUseClassMapperWhenProvidedOutbound() {
		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setTrustedPackages(TRUSTED_PACKAGE);

		converter.setClassMapper(classMapper);
		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(trade).isEqualTo(marshalledTrade);
	}

	@Test
	public void testAmqp330StringArray() {
		String[] testData = { "test" };
		Message message = converter.toMessage(testData, new MessageProperties());

		assertThat(testData).containsExactly((String[]) converter.fromMessage(message));
	}

	@Test
	public void testAmqp330ObjectArray() {
		SimpleTrade[] testData = { trade };
		Message message = converter.toMessage(testData, new MessageProperties());
		assertThat(testData).containsExactly((SimpleTrade[]) converter.fromMessage(message));
	}

	@Test
	public void testDefaultType() {
		byte[] bytes = "<root><name>foo</name></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		Message message = new Message(bytes, messageProperties);
		Jackson2XmlMessageConverter converter = new Jackson2XmlMessageConverter();
		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setDefaultType(Foo.class);
		converter.setClassMapper(classMapper);
		Object foo = converter.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);
	}

	@Test
	public void testDefaultTypeConfig() {
		byte[] bytes = "<root><name>foo</name></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		Message message = new Message(bytes, messageProperties);
		Object foo = xmlConverterWithDefaultType.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);
	}

	@Test
	public void testNoJsonContentType() {
		byte[] bytes = "<root><name>foo</name><root/>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		Message message = new Message(bytes, messageProperties);
		this.xmlConverterWithDefaultType.setAssumeSupportedContentType(false);
		Object foo = this.xmlConverterWithDefaultType.fromMessage(message);
		assertThat(foo).isEqualTo(bytes);
	}

	@Test
	public void testNoTypeInfo() {
		byte[] bytes = "<root><name><foo>bar</foo></name></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		Message message = new Message(bytes, messageProperties);
		Object wrapper = this.converter.fromMessage(message);
		assertThat(wrapper).isInstanceOf(LinkedHashMap.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) wrapper;
		assertThat(map.get("name")).isInstanceOf(LinkedHashMap.class);
	}

	@Test
	public void testInferredTypeInfo() {
		byte[] bytes = "<root><name>foo</name></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		messageProperties.setInferredArgumentType(Foo.class);
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);
	}

	@Test
	public void testInferredGenericTypeInfo() throws Exception {
		byte[] bytes = "<root><name>foo</name></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		messageProperties.setInferredArgumentType((new ParameterizedTypeReference<List<Foo>>() {

		}).getType());
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(List.class);
		assertThat(((List<?>) foo).get(0)).isInstanceOf(Foo.class);
	}

	@Test
	public void testInferredGenericMap1() {
		byte[] bytes = "<root><qux><element><foo><name>bar</name></foo></element></qux></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		messageProperties.setInferredArgumentType(
				(new ParameterizedTypeReference<Map<String, List<Bar>>>() {

				}).getType());
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(LinkedHashMap.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) foo;
		assertThat(map.get("qux")).isInstanceOf(List.class);
		Object row = ((List<?>) map.get("qux")).get(0);
		assertThat(row).isInstanceOf(Bar.class);
		assertThat(((Bar) row).getFoo()).isEqualTo(new Foo("bar"));
	}

	@Test
	public void testInferredGenericMap2() {
		byte[] bytes = "<root><qux><baz><foo><name>bar</name></foo></baz></qux></root>".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/xml");
		messageProperties.setInferredArgumentType(
				(new ParameterizedTypeReference<Map<String, Map<String, Bar>>>() {

				}).getType());
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(LinkedHashMap.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) foo;
		assertThat(map.get("qux")).isInstanceOf(Map.class);
		Object value = ((Map<?, ?>) map.get("qux")).get("baz");
		assertThat(value).isInstanceOf(Bar.class);
		assertThat(((Bar) value).getFoo()).isEqualTo(new Foo("bar"));
	}

	public static class Foo {

		private String name = "foo";

		public Foo() {
		}

		public Foo(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
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
			Foo other = (Foo) obj;
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			}
			else if (!name.equals(other.name)) {
				return false;
			}
			return true;
		}

	}

	public static class Bar {

		private String name = "bar";

		private Foo foo = new Foo();

		public Foo getFoo() {
			return foo;
		}

		public void setFoo(Foo foo) {
			this.foo = foo;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((foo == null) ? 0 : foo.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
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
			Bar other = (Bar) obj;
			if (foo == null) {
				if (other.foo != null) {
					return false;
				}
			}
			else if (!foo.equals(other.foo)) {
				return false;
			}
			if (name == null) {
				if (other.name != null) {
					return false;
				}
			}
			else if (!name.equals(other.name)) {
				return false;
			}
			return true;
		}

	}

}
