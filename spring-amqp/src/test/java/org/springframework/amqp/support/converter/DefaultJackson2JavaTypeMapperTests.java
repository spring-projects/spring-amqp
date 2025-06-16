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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.amqp.core.MessageProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.BDDMockito.given;

/**
 * @author James Carr
 * @author Sam Nelson
 * @author Andreas Asplund
 * @author Gary Russell
 * @author Artem Bilan
 */

@ExtendWith(MockitoExtension.class)
public class DefaultJackson2JavaTypeMapperTests {

	@Spy
	DefaultJackson2JavaTypeMapper javaTypeMapper = new DefaultJackson2JavaTypeMapper();

	private final MessageProperties properties = new MessageProperties();

	@SuppressWarnings("rawtypes")
	private final Class<ArrayList> containerClass = ArrayList.class;

	@SuppressWarnings("rawtypes")
	private final Class<HashMap> mapClass = HashMap.class;

	@BeforeEach
	public void setup() {
		this.javaTypeMapper.setTrustedPackages("org.springframework.amqp.support.converter");
	}

	@Test
	public void getAnObjectWhenClassIdNotPresent() {
		JavaType javaType = javaTypeMapper.toJavaType(properties);
		assertThat(javaType.getRawClass()).isEqualTo(Object.class);
	}

	@Test
	public void shouldLookInTheClassIdFieldNameToFindTheClassName() {
		properties.getHeaders().put("type", "java.lang.String");
		given(javaTypeMapper.getClassIdFieldName()).willReturn("type");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat(javaType).isEqualTo(TypeFactory.defaultInstance().constructType(String.class));
	}

	@Test
	public void shouldUseTheClassProvidedByTheLookupMapIfPresent() {
		properties.getHeaders().put("__TypeId__", "trade");
		javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat(TypeFactory.defaultInstance().constructType(SimpleTrade.class)).isEqualTo(javaType);
	}

	@Test
	public void fromJavaTypeShouldPopulateWithJavaTypeNameByDefault() {
		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructType(SimpleTrade.class), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className).isEqualTo(SimpleTrade.class.getName());
	}

	@Test
	public void shouldUseSpecialNameForClassIfPresent() {
		javaTypeMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructType(SimpleTrade.class), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className).isEqualTo("daytrade");
	}

	@Test
	public void shouldThrowAnExceptionWhenContentClassIdIsNotPresentWhenClassIdIsContainerType() {
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());

		try {
			javaTypeMapper.toJavaType(properties);
		}
		catch (MessageConversionException e) {
			String contentClassIdFieldName = javaTypeMapper.getContentClassIdFieldName();
			assertThat(e.getMessage()).contains("Could not resolve " + contentClassIdFieldName + " in header");
			return;
		}
		fail("Expected exception");
	}

	@Test
	public void shouldLookInTheContentClassIdFieldNameToFindTheContainerClassIDWhenClassIdIsContainerType() {
		properties.getHeaders().put("contentType", "java.lang.String");
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), ArrayList.class.getName());
		given(javaTypeMapper.getContentClassIdFieldName()).willReturn("contentType");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((CollectionType) javaType).isEqualTo(TypeFactory.defaultInstance().constructCollectionType(ArrayList.class, String.class));
	}

	@Test
	public void shouldUseTheContentClassProvidedByTheLookupMapIfPresent() {

		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), containerClass.getName());
		properties.getHeaders().put("__ContentTypeId__", "trade");

		Map<String, Class<?>> map = map("trade", SimpleTrade.class);
		map.put(javaTypeMapper.getClassIdFieldName(), containerClass);
		javaTypeMapper.setIdClassMapping(map);

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((CollectionType) javaType).isEqualTo(TypeFactory.defaultInstance().constructCollectionType(containerClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class)));
	}

	@Test
	public void fromJavaTypeShouldPopulateWithContentTypeJavaTypeNameByDefault() {

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructCollectionType(containerClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class)), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());

		assertThat(className).isEqualTo(ArrayList.class.getName());
		assertThat(contentClassName).isEqualTo(SimpleTrade.class.getName());
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
			assertThat(e.getMessage()).contains("Could not resolve " + contentClassIdFieldName + " in header");
			return;
		}
		fail("Expected exception");
	}

	@Test
	public void shouldLookInTheValueClassIdFieldNameToFindTheValueClassIDWhenClassIdIsAMap() {
		properties.getHeaders().put("keyType", "java.lang.Integer");
		properties.getHeaders().put(javaTypeMapper.getContentClassIdFieldName(), "java.lang.String");
		properties.getHeaders().put(javaTypeMapper.getClassIdFieldName(), HashMap.class.getName());
		given(javaTypeMapper.getKeyClassIdFieldName()).willReturn("keyType");

		JavaType javaType = javaTypeMapper.toJavaType(properties);

		assertThat((MapType) javaType).isEqualTo(TypeFactory.defaultInstance().constructMapType(HashMap.class, Integer.class, String.class));
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

		assertThat((MapType) javaType).isEqualTo(TypeFactory.defaultInstance().constructMapType(mapClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class),
				TypeFactory.defaultInstance().constructType(String.class)));
	}

	@Test
	public void fromJavaTypeShouldPopulateWithKeyTypeAndContentJavaTypeNameByDefault() {

		javaTypeMapper.fromJavaType(TypeFactory.defaultInstance().constructMapType(mapClass,
				TypeFactory.defaultInstance().constructType(SimpleTrade.class),
				TypeFactory.defaultInstance().constructType(String.class)), properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		String contentClassName = (String) properties.getHeaders().get(javaTypeMapper.getContentClassIdFieldName());
		String keyClassName = (String) properties.getHeaders().get(javaTypeMapper.getKeyClassIdFieldName());

		assertThat(className).isEqualTo(HashMap.class.getName());
		assertThat(contentClassName).isEqualTo(String.class.getName());
		assertThat(keyClassName).isEqualTo(SimpleTrade.class.getName());
	}

	@Test
	public void fromClassShouldPopulateWithJavaTypeNameByDefault() {
		javaTypeMapper.fromClass(SimpleTrade.class, properties);

		String className = (String) properties.getHeaders().get(javaTypeMapper.getClassIdFieldName());
		assertThat(className).isEqualTo(SimpleTrade.class.getName());
	}

	@Test
	public void toClassShouldUseTheClassProvidedByTheLookupMapIfPresent() {
		properties.getHeaders().put("__TypeId__", "trade");
		javaTypeMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		Class<?> clazz = javaTypeMapper.toClass(properties);

		assertThat(clazz).isEqualTo(SimpleTrade.class);
	}

	private Map<String, Class<?>> map(String string, Class<?> clazz) {
		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.put(string, clazz);
		return map;
	}
}
