/*
 * Copyright 2002-2017 the original author or authors.
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
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.amqp.core.MessageProperties;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;

/**
 * @author James Carr
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 * @author Artem Bilan
 */

@RunWith(MockitoJUnitRunner.class)
public class DefaultJackson2JavaTypeMapperTests {

	@Spy
	DefaultJackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	private final MessageProperties properties = new MessageProperties();

	@SuppressWarnings("rawtypes")
	private final Class<ArrayList> containerClass = ArrayList.class;

	@SuppressWarnings("rawtypes")
	private final Class<HashMap> mapClass = HashMap.class;

	@Before
	public void setup() {
		this.javaTypeMapper.setTrustedPackages("org.springframework.amqp.support.converter");
	}

	@Test
	public void getAnObjectWhenClassIdNotPresent() {
		JavaType javaType = javaTypeMapper.toJavaType(properties);
		assertEquals(Object.class, javaType.getRawClass());
	}

	@Test
	public void shouldLookInTheClassIdFieldNameToFindTheClassName() {
		properties.getHeaders().put("type", "java.lang.String");
		given(javaTypeMapper.getClassIdFieldName()).willReturn("type");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat(javaType, equalTo(TypeFactory.defaultInstance().constructType(String.class)));
	}

	@Test
	public void shouldUseTheClassProvidedByTheLookupMapIfPresent() {
		properties.getHeaders().put("__TypeId__", "trade");
		javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertEquals(javaType, TypeFactory.defaultInstance().constructType(SimpleTrade.class));
	}

	@Test
	public void fromJavaTypeShouldPopulateWithJavaTypeNameByDefault() {
		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructType(SimpleTrade.class), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className, equalTo(SimpleTrade.class.getName()));
	}

	@Test
	public void shouldUseSpecialNameForClassIfPresent() throws Exception {
		javaTypeMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructType(SimpleTrade.class), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className, equalTo("daytrade"));
	}

	@Test
	public void shouldThrowAnExceptionWhenContentClassIdIsNotPresentWhenClassIdIsContainerType() {
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());

		try {
			javaTypeMapper.toJavaType(properties);
		}
		catch (MessageConversionException e) {
			String contentClassIdFieldName = javaTypeMapper.getContentClassIdFieldName();
			assertThat(e.getMessage(), containsString("Could not resolve " + contentClassIdFieldName + " in header"));
			return;
		}
		fail();
	}

	@Test
	public void shouldLookInTheContentClassIdFieldNameToFindTheContainerClassIDWhenClassIdIsContainerType() {
		properties.getHeaders().put("contentType", "java.lang.String");
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());
		given(javaTypeMapper.getContentClassIdFieldName()).willReturn("contentType");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((CollectionType) javaType,
				equalTo(TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, String.class)));
	}

	@Test
	public void shouldUseTheContentClassProvidedByTheLookupMapIfPresent() {

		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), containerClass.getName());
		properties.getHeaders().put("__ContentTypeId__", "trade");

		Map<String, Class<?>> map = map("trade", SimpleTrade.class);
		map.put(javaTypeMapper.getClassIdFieldName(), containerClass);
		javaTypeMapper.setIdClassMapping(map);

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((CollectionType) javaType,
				equalTo(TypeFactory.defaultInstance().constructCollectionType(containerClass,
						TypeFactory.defaultInstance().constructType(SimpleTrade.class))));
	}

	@Test
	public void fromJavaTypeShouldPopulateWithContentTypeJavaTypeNameByDefault() {

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructCollectionType(containerClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class)), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());

		assertThat(className, equalTo(ArrayList.class.getName()));
		assertThat(contentClassName, equalTo(SimpleTrade.class.getName()));
	}

	@Test
	public void shouldThrowAnExceptionWhenKeyClassIdIsNotPresentWhenClassIdIsAMap() {
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), HashMap.class.getName());
		properties.getHeaders().put(javaTypeMapper.getKeyClassIdFieldName(), String.class.getName());

		try {
			javaTypeMapper.toJavaType(properties);
		}
		catch (MessageConversionException e) {
			String contentClassIdFieldName = javaTypeMapper.getContentClassIdFieldName();
			assertThat(e.getMessage(), containsString("Could not resolve " + contentClassIdFieldName + " in header"));
			return;
		}
		fail();
	}

	@Test
	public void shouldLookInTheValueClassIdFieldNameToFindTheValueClassIDWhenClassIdIsAMap() {
		properties.getHeaders().put("keyType", "java.lang.Integer");
		properties.getHeaders().put(javaTypeMapper.getContentClassIdFieldName(), "java.lang.String");
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), HashMap.class.getName());
		given(javaTypeMapper.getKeyClassIdFieldName()).willReturn("keyType");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((MapType) javaType,
				equalTo(TypeFactory.defaultInstance().constructMapType(HashMap.class, Integer.class, String.class)));
	}

	@Test
	public void shouldUseTheKeyClassProvidedByTheLookupMapIfPresent() {
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), mapClass.getName());
		properties.getHeaders().put(javaTypeMapper.getContentClassIdFieldName(), "java.lang.String");
		properties.getHeaders().put("__KeyTypeId__", "trade");

		Map<String, Class<?>> map = map("trade", SimpleTrade.class);
		map.put(javaTypeMapper.getClassIdFieldName(), mapClass);
		map.put(javaTypeMapper.getContentClassIdFieldName(), String.class);
		javaTypeMapper.setIdClassMapping(map);

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((MapType) javaType,
				equalTo(TypeFactory.defaultInstance().constructMapType(mapClass,
						TypeFactory.defaultInstance().constructType(SimpleTrade.class),
						TypeFactory.defaultInstance().constructType(String.class))));
	}

	@Test
	public void fromJavaTypeShouldPopulateWithKeyTypeAndContentJavaTypeNameByDefault() {

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructMapType(mapClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class),
				TypeFactory.defaultInstance().constructType(String.class)), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());
		String keyClassName = (String) properties.getHeaders().get(javaTypeMapper.getKeyClassIdFieldName());

		assertThat(className, equalTo(HashMap.class.getName()));
		assertThat(contentClassName, equalTo(String.class.getName()));
		assertThat(keyClassName, equalTo(SimpleTrade.class.getName()));
	}

	@Test
	public void fromClassShouldPopulateWithJavaTypeNameByDefault() {
		javaTypeMapper.fromClass(SimpleTrade.class, properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className, equalTo(SimpleTrade.class.getName()));
	}

	@Test
	public void toClassShouldUseTheClassProvidedByTheLookupMapIfPresent() {
		properties.getHeaders().put("__TypeId__", "trade");
		javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		Class<?> clazz = javaTypeMapper.toClass(properties);

		assertEquals(SimpleTrade.class, clazz);
	}

	private Map<String, Class<?>> map(String string, Class<?> clazz) {
		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.put(string, clazz);
		return map;
	}
}
