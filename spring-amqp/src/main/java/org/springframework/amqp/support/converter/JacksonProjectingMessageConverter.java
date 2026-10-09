/*
 * Copyright 2019-present the original author or authors.
 */

package org.springframework.amqp.support.converter;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Type;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.mapper.MappingException;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.json.JsonMapper;

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
 * Based on Jackson 3.
 *
 * @author Artem Bilan
 *
 * @since 4.0
 *
 */
public class JacksonProjectingMessageConverter {

	private final ProjectionFactory projectionFactory;

	public JacksonProjectingMessageConverter(JsonMapper mapper) {
		Assert.notNull(mapper, "'mapper' cannot be null");
		MappingProvider provider = new Jackson3MappingProvider(mapper);
		MethodInterceptorFactory interceptorFactory = new JsonProjectingMethodInterceptorFactory(provider);

		SpelAwareProxyProjectionFactory factory = new SpelAwareProxyProjectionFactory();
		factory.registerMethodInvokerFactory(interceptorFactory);

		this.projectionFactory = factory;
	}

	public Object convert(Message message, Type type) {
		return this.projectionFactory.createProjection(ResolvableType.forType(type).resolve(Object.class),
				new ByteArrayInputStream(message.getBody()));
	}

	/**
	 * A {@link MappingProvider} implementation for Jackson 3.
	 * Until respective implementation is there in json-path library.
	 * @param jsonMapper Jackson 3 {@link JsonMapper}
	 */
	private record Jackson3MappingProvider(JsonMapper jsonMapper) implements MappingProvider {

		@Override
		public <T> @Nullable T map(@Nullable Object source, Class<T> targetType, Configuration configuration) {
			if (source == null) {
				return null;
			}
			try {
				return this.jsonMapper.convertValue(source, targetType);
			}
			catch (Exception ex) {
				throw new MappingException(ex);
			}
		}

		@Override
		public <T> @Nullable T map(@Nullable Object source, final TypeRef<T> targetType, Configuration configuration) {
			if (source == null) {
				return null;
			}
			JavaType type = this.jsonMapper.constructType(targetType.getType());

			try {
				return this.jsonMapper.convertValue(source, type);
			}
			catch (Exception ex) {
				throw new MappingException(ex);
			}
		}

	}

}
