/*
 * Copyright 2019-present the original author or authors.
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

import java.io.ByteArrayInputStream;
import java.lang.reflect.Type;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;

import org.springframework.amqp.core.Message;
import org.springframework.core.ResolvableType;
import org.springframework.data.projection.MethodInterceptorFactory;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.web.JsonProjectingMethodInterceptorFactory;
import org.springframework.util.Assert;

/**
 * Uses a Spring Data {@link ProjectionFactory} to bind incoming messages to projection
 * interfaces.
 *
 * @author Gary Russell
 *
 * @since 2.2
 *
 * @deprecated since 4.0 in favor of {@link JacksonProjectingMessageConverter}.
 */
@Deprecated(since = "4.0", forRemoval = true)
public class ProjectingMessageConverter {

	private final ProjectionFactory projectionFactory;

	public ProjectingMessageConverter(ObjectMapper mapper) {
		Assert.notNull(mapper, "'mapper' cannot be null");
		JacksonMappingProvider provider = new JacksonMappingProvider(mapper);
		MethodInterceptorFactory interceptorFactory = new JsonProjectingMethodInterceptorFactory(provider);

		SpelAwareProxyProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.registerMethodInvokerFactory(interceptorFactory);

		this.projectionFactory = factory;
	}

	public Object convert(Message message, Type type) {
		return this.projectionFactory.createProjection(ResolvableType.forType(type).resolve(Object.class),
				new ByteArrayInputStream(message.getBody()));
	}

}
