/*
 * Copyright 2023-present the original author or authors.
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import org.springframework.util.ClassUtils;


/**
 * The utilities for Jackson {@link ObjectMapper} instances.
 *
 * @author Artem Bilan
 *
 * @since 3.1.1
 *
 * @deprecated since 4.0 in favor of native Jackson 3 {@link tools.jackson.databind.json.JsonMapper#builder()} API.
 */
@Deprecated(forRemoval = true, since = "4.0")
public final class JacksonUtils {

	private static final boolean JDK8_MODULE_PRESENT =
			ClassUtils.isPresent("com.fasterxml.jackson.datatype.jdk8.Jdk8Module", null);

	private static final boolean PARAMETER_NAMES_MODULE_PRESENT =
			ClassUtils.isPresent("com.fasterxml.jackson.module.paramnames.ParameterNamesModule", null);

	private static final boolean JAVA_TIME_MODULE_PRESENT =
			ClassUtils.isPresent("com.fasterxml.jackson.datatype.jsr310.JavaTimeModule", null);

	private static final boolean JODA_MODULE_PRESENT =
			ClassUtils.isPresent("com.fasterxml.jackson.datatype.joda.JodaModule", null);

	private static final boolean KOTLIN_MODULE_PRESENT =
			ClassUtils.isPresent("kotlin.Unit", null) &&
					ClassUtils.isPresent("com.fasterxml.jackson.module.kotlin.KotlinModule", null);

	/**
	 * Factory for {@link ObjectMapper} instances with registered well-known modules
	 * and disabled {@link MapperFeature#DEFAULT_VIEW_INCLUSION} and
	 * {@link DeserializationFeature#FAIL_ON_UNKNOWN_PROPERTIES} features.
	 * @return the {@link ObjectMapper} instance.
	 */
	public static ObjectMapper enhancedObjectMapper() {
		ObjectMapper objectMapper = JsonMapper.builder()
				.configure(MapperFeature.DEFAULT_VIEW_INCLUSION, false)
				.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
				.build();
		registerWellKnownModulesIfAvailable(objectMapper);
		return objectMapper;
	}

	private static void registerWellKnownModulesIfAvailable(ObjectMapper objectMapper) {
		if (JDK8_MODULE_PRESENT) {
			objectMapper.registerModule(Jdk8ModuleProvider.MODULE);
		}

		if (PARAMETER_NAMES_MODULE_PRESENT) {
			objectMapper.registerModule(ParameterNamesProvider.MODULE);
		}

		if (JAVA_TIME_MODULE_PRESENT) {
			objectMapper.registerModule(JavaTimeModuleProvider.MODULE);
		}

		if (JODA_MODULE_PRESENT) {
			objectMapper.registerModule(JodaModuleProvider.MODULE);
		}

		if (KOTLIN_MODULE_PRESENT) {
			objectMapper.registerModule(KotlinModuleProvider.MODULE);
		}
	}

	private JacksonUtils() {
	}

	private static final class Jdk8ModuleProvider {

		static final com.fasterxml.jackson.databind.Module MODULE =
				new com.fasterxml.jackson.datatype.jdk8.Jdk8Module();

	}

	private static final class ParameterNamesProvider {

		static final com.fasterxml.jackson.databind.Module MODULE =
				new com.fasterxml.jackson.module.paramnames.ParameterNamesModule();

	}

	private static final class JavaTimeModuleProvider {

		static final com.fasterxml.jackson.databind.Module MODULE =
				new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule();

	}

	private static final class JodaModuleProvider {

		static final com.fasterxml.jackson.databind.Module MODULE =
				new com.fasterxml.jackson.datatype.joda.JodaModule();

	}

	private static final class KotlinModuleProvider {

		static final com.fasterxml.jackson.databind.Module MODULE =
				new com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build();

	}

}
