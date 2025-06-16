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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.amqp.core.MessageProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

/**
 * @author James Carr
 * @author Gary Russell
 *
 */
@ExtendWith(MockitoExtension.class)
public class DefaultClassMapperTests {

	@Spy
	private final DefaultClassMapper classMapper = new DefaultClassMapper();

	private final MessageProperties props = new MessageProperties();

	@Test
	public void shouldThrowAnExceptionWhenClassIdNotPresent() {
		try {
			classMapper.toClass(props);
		}
		catch (MessageConversionException e) {
			String classIdFieldName = classMapper.getClassIdFieldName();
			assertThat(e.getMessage()).contains("Could not resolve "
					+ classIdFieldName + " in header");
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldLookInTheClassIdFieldNameToFindTheClassName() {
		props.getHeaders().put("type", "java.lang.String");
		given(classMapper.getClassIdFieldName()).willReturn("type");

		Class<String> clazz = (Class<String>) classMapper.toClass(props);

		assertThat(clazz).isEqualTo(String.class);
	}

	@Test
	public void shouldUseTheCLassProvidedByTheLookupMapIfPresent() {
		props.getHeaders().put("__TypeId__", "trade");
		classMapper.setIdClassMapping(map("trade", SimpleTrade.class));

		@SuppressWarnings("rawtypes")
		Class clazz = classMapper.toClass(props);

		assertThat(SimpleTrade.class).isEqualTo(clazz);

	}

	@Test
	public void shouldReturnLinkedHashMapForFieldWithHashtable() {
		props.getHeaders().put("__TypeId__", "Hashtable");

		Class<?> clazz = classMapper.toClass(props);

		assertThat(clazz).isEqualTo(LinkedHashMap.class);
	}

	@Test
	public void fromClassShouldPopulateWithClassNameByDefault() {
		classMapper.fromClass(SimpleTrade.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());
		assertThat(className).isEqualTo(SimpleTrade.class.getName());
	}

	@Test
	public void shouldUseSpecialNameForClassIfPresent() {
		classMapper.setIdClassMapping(map("daytrade", SimpleTrade.class));
		classMapper.afterPropertiesSet();

		classMapper.fromClass(SimpleTrade.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());
		assertThat(className).isEqualTo("daytrade");
	}

	@Test
	public void shouldConvertAnyMapToUseHashtables() {
		classMapper.fromClass(LinkedHashMap.class, props);

		String className = (String) props.getHeaders().get(
				classMapper.getClassIdFieldName());

		assertThat(className).isEqualTo("Hashtable");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldUseDefaultType() {
		props.getHeaders().clear();
		classMapper.setDefaultType(Foo.class);
		Class<Foo> clazz = (Class<Foo>) classMapper.toClass(props);

		assertThat(clazz).isSameAs(Foo.class);
		classMapper.setDefaultType(LinkedHashMap.class);
	}

	private Map<String, Class<?>> map(String string, Class<?> class1) {
		Map<String, Class<?>> map = new HashMap<String, Class<?>>();
		map.put(string, class1);
		return map;
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
