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

import java.math.BigDecimal;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.deser.std.StdDeserializer;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.module.SimpleModule;
import tools.jackson.databind.ser.BeanSerializerFactory;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.web.JsonPath;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mark Pollack
 * @author Dave Syer
 * @author Sam Nelson
 * @author Gary Russell
 * @author Andreas Asplund
 * @author Artem Bilan
 */
@SpringJUnitConfig
@DirtiesContext
public class JacksonJsonMessageConverterTests {

	public static final String TRUSTED_PACKAGE = JacksonJsonMessageConverterTests.class.getPackage().getName();

	private JacksonJsonMessageConverter converter;

	private SimpleTrade trade;

	@Autowired
	private JacksonJsonMessageConverter jsonConverterWithDefaultType;

	@BeforeEach
	public void before() {
		converter = new JacksonJsonMessageConverter(TRUSTED_PACKAGE);
		trade = new SimpleTrade();
		trade.setAccountName("Acct1");
		trade.setBuyRequest(true);
		trade.setOrderType("Market");
		trade.setPrice(new BigDecimal("103.30"));
		trade.setQuantity(100);
		trade.setRequestId("R123");
		trade.setTicker("VMW");
		trade.setUserName("Joe Trader");
	}

	@Test
	public void simpleTrade() {
		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);
	}

	@Test
	public void simpleTradeOverrideMapper() {
		ObjectMapper mapper =
				JsonMapper.builder()
						.serializerFactory(BeanSerializerFactory.instance)
						.build();
		converter = new JacksonJsonMessageConverter(mapper);

		((DefaultJacksonJavaTypeMapper) this.converter.getJavaTypeMapper())
				.setTrustedPackages(TRUSTED_PACKAGE);

		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);
	}

	@Test
	public void nestedBean() {
		Bar bar = new Bar();
		bar.getFoo().setName("spam");

		Message message = converter.toMessage(bar, new MessageProperties());

		Bar marshalled = (Bar) converter.fromMessage(message);
		assertThat(marshalled).isEqualTo(bar);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void hashtable() {
		Hashtable<String, String> hashtable = new Hashtable<>();
		hashtable.put("TICKER", "VMW");
		hashtable.put("PRICE", "103.2");

		Message message = converter.toMessage(hashtable, new MessageProperties());
		Hashtable<String, String> marhsalledHashtable = (Hashtable<String, String>) converter.fromMessage(message);

		assertThat(marhsalledHashtable.get("TICKER")).isEqualTo("VMW");
		assertThat(marhsalledHashtable.get("PRICE")).isEqualTo("103.2");
	}

	@Test
	public void shouldUseClassMapperWhenProvided() {
		Message message = converter.toMessage(trade, new MessageProperties());

		converter.setClassMapper(new DefaultClassMapper());

		((DefaultClassMapper) this.converter.getClassMapper()).setTrustedPackages(TRUSTED_PACKAGE);

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);
	}

	@Test
	public void shouldUseClassMapperWhenProvidedOutbound() {
		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setTrustedPackages(TRUSTED_PACKAGE);

		converter.setClassMapper(classMapper);
		Message message = converter.toMessage(trade, new MessageProperties());

		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);
	}

	@Test
	public void testAmqp330StringArray() {
		String[] testData = {"test"};
		Message message = converter.toMessage(testData, new MessageProperties());
		assertThat((Object[]) converter.fromMessage(message)).isEqualTo(testData);
	}

	@Test
	public void testAmqp330ObjectArray() {
		SimpleTrade[] testData = {trade};
		Message message = converter.toMessage(testData, new MessageProperties());
		assertThat((Object[]) converter.fromMessage(message)).isEqualTo(testData);
	}

	@Test
	public void testDefaultType() {
		byte[] bytes = "{\"name\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		Message message = new Message(bytes, messageProperties);
		JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setDefaultType(Foo.class);
		converter.setClassMapper(classMapper);
		Object foo = converter.fromMessage(message);
		assertThat(foo instanceof Foo).isTrue();
	}

	@Test
	public void testDefaultTypeConfig() {
		byte[] bytes = "{\"name\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		Message message = new Message(bytes, messageProperties);
		Object foo = jsonConverterWithDefaultType.fromMessage(message);
		assertThat(foo instanceof Foo).isTrue();
	}

	@Test
	public void testNoJsonContentType() {
		byte[] bytes = "{\"name\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		Message message = new Message(bytes, messageProperties);
		this.jsonConverterWithDefaultType.setAssumeSupportedContentType(false);
		Object foo = this.jsonConverterWithDefaultType.fromMessage(message);
		assertThat(foo).isEqualTo(bytes);
	}

	@Test
	public void testNoTypeInfo() {
		byte[] bytes = "{\"name\" : { \"foo\" : \"bar\" } }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(LinkedHashMap.class);
		@SuppressWarnings("unchecked")
		Map<String, Object> map = (Map<String, Object>) foo;
		assertThat(map.get("name")).isInstanceOf(LinkedHashMap.class);
	}

	@Test
	public void testInferredTypeInfo() {
		byte[] bytes = "{\"name\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		messageProperties.setInferredArgumentType(Foo.class);
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);
	}

	@Test
	public void testInferredGenericTypeInfo() throws Exception {
		byte[] bytes = "[ {\"name\" : \"foo\" } ]".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
		messageProperties.setInferredArgumentType((new ParameterizedTypeReference<List<Foo>>() {

		}).getType());
		Message message = new Message(bytes, messageProperties);
		Object foo = this.converter.fromMessage(message);
		assertThat(foo).isInstanceOf(List.class);
		assertThat(((List<?>) foo).get(0)).isInstanceOf(Foo.class);
	}

	@Test
	public void testInferredGenericMap1() {
		byte[] bytes = "{\"qux\" : [ { \"foo\" : { \"name\" : \"bar\" } } ] }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
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
		byte[] bytes = "{\"qux\" : { \"baz\" : { \"foo\" : { \"name\" : \"bar\" } } } }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setContentType("application/json");
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

	@Test
	public void testProjection() {
		JacksonJsonMessageConverter conv = new JacksonJsonMessageConverter();
		conv.setUseProjectionForInterfaces(true);
		MessageProperties properties = new MessageProperties();
		properties.setInferredArgumentType(Sample.class);
		properties.setContentType("application/json");
		Message message = new Message(
				"{ \"username\" : \"SomeUsername\", \"user\" : { \"name\" : \"SomeName\"}}".getBytes(), properties);
		Object fromMessage = conv.fromMessage(message);
		assertThat(fromMessage).isInstanceOf(Sample.class);
		assertThat(((Sample) fromMessage).getUsername()).isEqualTo("SomeUsername");
		assertThat(((Sample) fromMessage).getName()).isEqualTo("SomeName");
	}

	@Test
	public void testMissingContentType() {
		byte[] bytes = "{\"name\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		Message message = new Message(bytes, messageProperties);
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter();
		DefaultClassMapper classMapper = new DefaultClassMapper();
		classMapper.setDefaultType(Foo.class);
		jsonMessageConverter.setClassMapper(classMapper);
		Object foo = jsonMessageConverter.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);

		foo = jsonMessageConverter.fromMessage(message);
		assertThat(foo).isInstanceOf(Foo.class);

		jsonMessageConverter.setAssumeSupportedContentType(false);
		foo = jsonMessageConverter.fromMessage(message);
		assertThat(foo).isSameAs(bytes);
	}

	@Test
	void customAbstractClass() {
		byte[] bytes = "{\"field\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("__TypeId__", String.class.getName());
		messageProperties.setInferredArgumentType(Baz.class);
		Message message = new Message(bytes, messageProperties);
		ObjectMapper mapper = JsonMapper.builder().addModule(new BazModule()).build();
		JacksonJsonMessageConverter messageConverter = new JacksonJsonMessageConverter(mapper);
		messageConverter.setAlwaysConvertToInferredType(true);
		Baz baz = (Baz) messageConverter.fromMessage(message);
		assertThat(((Qux) baz).getField()).isEqualTo("foo");
	}

	@Test
	void fallbackToHeaders() {
		byte[] bytes = "{\"field\" : \"foo\" }".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("__TypeId__", Buz.class.getName());
		messageProperties.setInferredArgumentType(Baz.class);
		Message message = new Message(bytes, messageProperties);
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter();
		Fiz buz = (Fiz) jsonMessageConverter.fromMessage(message);
		assertThat(((Buz) buz).getField()).isEqualTo("foo");
	}

	@Test
	void customAbstractClassList() throws Exception {
		byte[] bytes = "[{\"field\" : \"foo\" }]".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setHeader("__TypeId__", String.class.getName());
		messageProperties.setInferredArgumentType(getClass().getDeclaredMethod("bazLister").getGenericReturnType());
		Message message = new Message(bytes, messageProperties);
		ObjectMapper mapper = JsonMapper.builder().addModule(new BazModule()).build();
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter(mapper);
		jsonMessageConverter.setAlwaysConvertToInferredType(true);
		@SuppressWarnings("unchecked")
		List<Baz> bazs = (List<Baz>) jsonMessageConverter.fromMessage(message);
		assertThat(bazs).hasSize(1);
		assertThat(((Qux) bazs.get(0)).getField()).isEqualTo("foo");
	}

	@Test
	void cantDeserializeFizListUseHeaders() throws Exception {
		byte[] bytes = "[{\"field\" : \"foo\" }]".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setInferredArgumentType(getClass().getDeclaredMethod("fizLister").getGenericReturnType());
		messageProperties.setHeader("__TypeId__", List.class.getName());
		messageProperties.setHeader("__ContentTypeId__", Buz.class.getName());
		Message message = new Message(bytes, messageProperties);
		ObjectMapper mapper = JsonMapper.builder().addModule(new BazModule()).build();
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter(mapper);
		@SuppressWarnings("unchecked")
		List<Fiz> buzs = (List<Fiz>) jsonMessageConverter.fromMessage(message);
		assertThat(buzs).hasSize(1);
		assertThat(((Buz) buzs.get(0)).getField()).isEqualTo("foo");
	}

	@Test
	void concreteInListRegression() throws Exception {
		byte[] bytes = "[{\"name\":\"bar\"}]".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setInferredArgumentType(getClass().getDeclaredMethod("fooLister").getGenericReturnType());
		messageProperties.setHeader("__TypeId__", List.class.getName());
		messageProperties.setHeader("__ContentTypeId__", Object.class.getName());
		Message message = new Message(bytes, messageProperties);
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter();
		@SuppressWarnings("unchecked")
		List<Foo> foos = (List<Foo>) jsonMessageConverter.fromMessage(message);
		assertThat(foos).hasSize(1);
		assertThat(foos.get(0).getName()).isEqualTo("bar");
	}

	@Test
	void concreteInMapRegression() throws Exception {
		byte[] bytes = "{\"test\":{\"field\":\"baz\"}}".getBytes();
		MessageProperties messageProperties = new MessageProperties();
		messageProperties.setInferredArgumentType(getClass().getDeclaredMethod("stringQuxLister").getGenericReturnType());
		messageProperties.setHeader("__TypeId__", Map.class.getName());
		messageProperties.setHeader("__KeyTypeId__", String.class.getName());
		messageProperties.setHeader("__ContentTypeId__", Object.class.getName());
		Message message = new Message(bytes, messageProperties);
		JacksonJsonMessageConverter jsonMessageConverter = new JacksonJsonMessageConverter();

		@SuppressWarnings("unchecked")
		Map<String, Qux> foos = (Map<String, Qux>) jsonMessageConverter.fromMessage(message);
		assertThat(foos).hasSize(1);
		assertThat(foos.keySet().iterator().next()).isEqualTo("test");
		assertThat(foos.values().iterator().next().getField()).isEqualTo("baz");
	}

	@Test
	void charsetInContentType() {
		trade.setUserName("John Doe ∫");
		JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
		String utf8 = "application/json;charset=utf-8";
		converter.setSupportedContentType(MimeTypeUtils.parseMimeType(utf8));
		Message message = converter.toMessage(trade, new MessageProperties());
		int bodyLength8 = message.getBody().length;
		assertThat(message.getMessageProperties().getContentEncoding()).isNull();
		assertThat(message.getMessageProperties().getContentType()).isEqualTo(utf8);
		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);

		// use content type property
		String utf16 = "application/json;charset=utf-16";
		converter.setSupportedContentType(MimeTypeUtils.parseMimeType(utf16));
		message = converter.toMessage(trade, new MessageProperties());
		assertThat(message.getBody().length).isNotEqualTo(bodyLength8);
		assertThat(message.getMessageProperties().getContentEncoding()).isNull();
		assertThat(message.getMessageProperties().getContentType()).isEqualTo(utf16);
		marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);

		// no encoding in message, use configured default
		converter.setSupportedContentType(MimeTypeUtils.parseMimeType("application/json"));
		converter.setDefaultCharset("UTF-16");
		message = converter.toMessage(trade, new MessageProperties());
		assertThat(message.getBody().length).isNotEqualTo(bodyLength8);
		assertThat(message.getMessageProperties().getContentEncoding()).isNotNull();
		message.getMessageProperties().setContentEncoding(null);
		marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);

	}

	@Test
	void noConfigForCharsetInContentType() {
		trade.setUserName("John Doe ∫");
		JacksonJsonMessageConverter converter = new JacksonJsonMessageConverter();
		Message message = converter.toMessage(trade, new MessageProperties());
		int bodyLength8 = message.getBody().length;
		SimpleTrade marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);

		// no encoding in message; use configured default
		message = converter.toMessage(trade, new MessageProperties());
		assertThat(message.getMessageProperties().getContentEncoding()).isNotNull();
		message.getMessageProperties().setContentEncoding(null);
		marshalledTrade = (SimpleTrade) converter.fromMessage(message);
		assertThat(marshalledTrade).isEqualTo(trade);

		converter.setDefaultCharset("UTF-16");
		Message message2 = converter.toMessage(trade, new MessageProperties());
		message2.getMessageProperties().setContentEncoding(null);
		assertThat(message2.getBody().length).isNotEqualTo(bodyLength8);
		converter.setDefaultCharset("UTF-8");

		assertThat(converter.fromMessage(message2)).isEqualTo(trade);
	}

	public List<Foo> fooLister() {
		return null;
	}

	public Map<String, Qux> stringQuxLister() {
		return null;
	}

	public List<Baz> bazLister() {
		return null;
	}

	public List<Fiz> fizLister() {
		return null;
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

	interface Sample {

		String getUsername();

		@JsonPath("$.user.name")
		String getName();

	}

	public interface Baz {

	}

	public static class Qux implements Baz {

		private String field;

		public Qux() {
		}

		public Qux(String field) {
			this.field = field;
		}

		public String getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field = field;
		}

	}

	@SuppressWarnings("serial")
	public static class BazDeserializer extends StdDeserializer<Baz> {

		public BazDeserializer() {
			super(Baz.class);
		}

		@Override
		public Baz deserialize(JsonParser p, DeserializationContext ctxt) throws JacksonException {
			p.nextName();
			String field = p.nextStringValue();
			p.nextToken();
			return new Qux(field);
		}

	}

	public interface Fiz {

	}

	public static class Buz implements Fiz {

		private String field;

		public String getField() {
			return this.field;
		}

		public void setField(String field) {
			this.field = field;
		}

	}

	@SuppressWarnings("serial")
	public static class BazModule extends SimpleModule {

		@SuppressWarnings("this-escape")
		public BazModule() {
			addDeserializer(Baz.class, new BazDeserializer());
		}

	}

}
