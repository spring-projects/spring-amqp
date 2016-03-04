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

package org.springframework.amqp.support.converter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.mockito.BDDMockito.given;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.JsonMessageConverterTests.Foo;

/**
 * @author James Carr
 * @author Gary Russell
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultClassMapperTest {
	@Spy
	DefaultClassMapper classMapper = new DefaultClassMapper();

	private final MessageProperties props = new MessageProperties();

	@Test
	public void shouldThrowAnExceptionWhenClassIdNotPresent() {
		try {
			classMapper.toClass(props);
		}
		catch (MessageConversionException e) {
			String classIdFieldName = classMapper.getClassIdFieldName();
			assertThat(e.getMessage(), containsString("Could not resolve "
					+ classIdFieldName + " in header"));
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldLookInTheClassIdFieldNameToFindTheClassName() {
		props.getHeaders().put("type", "java.lang.String");
		given(classMapper.getClassIdFieldName()).willReturn("type");

		Class<String> clazz = (Class<String>) classMapper.toClass(props);

		assertThat(clazz, equalTo(String.class));
	}

	@Test
	public void shouldUseTheCLassProvidedByTheLookupMapIfPresent() {
		props.getHeaders().put("__TypeId__", "trade");
		classMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		@SuppressWarnings("rawtypes")
		Class clazz = classMapper.toClass(props);

		assertEquals(clazz, SimpleTrade.class);

	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldReturnHashtableForFieldWithHashtable() {
		props.getHeaders().put("__TypeId__", "Hashtable");

		@SuppressWarnings("rawtypes")
		Class<Hashtable> clazz = (Class<Hashtable>) classMapper.toClass(props);

		assertThat(clazz, equalTo(Hashtable.class));
	}

	@Test
	public void fromClassShouldPopulateWithClassNameByDefailt() {
		classMapper.fromClass(SimpleTrade.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());
		assertThat(className, equalTo(SimpleTrade.class.getName()));
	}

	@Test
	public void shouldUseSpecialnameForClassIfPresent() throws Exception {
		classMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));
		classMapper.afterPropertiesSet();

		classMapper.fromClass(SimpleTrade.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());
		assertThat(className, equalTo("daytrade"));
	}

	@Test
	public void shouldConvertAnyMapToUseHashtables() {
		classMapper.fromClass(LinkedHashMap.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());

		assertThat(className, equalTo("Hashtable"));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldUseDefaultType() {
		props.getHeaders().clear();
		classMapper.setDefaultType(Foo.class);
		Class<Foo> clazz = (Class<Foo>) classMapper.toClass(props);

		assertSame(Foo.class, clazz);
		classMapper.setDefaultType(null);
	}

	private Map<String, Class<?>> map(String string, Class<?> class1) {
		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.put(string, class1);
		return map;
	}

}
