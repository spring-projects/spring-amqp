/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.amqp.client.config;

import java.util.Map;

import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.ClientOptions;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.context.annotation.Role;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * The {@link Configuration} for AMQP 1.0 infrastructure.
 * Provides default bean for the ProtonJ {@link Client}.
 * If this class is imported via {@link EnableAmqp}, then {@link ClientOptions} are
 * based on {@link EnableAmqp} attributes.
 *
 * @author Artem Bilan
 *
 * @since 4.1
 *
 * @see EnableAmqp
 */
@Configuration(proxyBeanMethods = false)
@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
public class AmqpDefaultConfiguration implements ImportAware, EnvironmentAware {

	private @Nullable AnnotationAttributes attributes;

	@SuppressWarnings("NullAway.Init")
	private Environment environment;

	@Override
	public void setImportMetadata(AnnotationMetadata importMetadata) {
		Map<String, @Nullable Object> map = importMetadata.getAnnotationAttributes(EnableAmqp.class.getName());
		AnnotationAttributes attributes = AnnotationAttributes.fromMap(map);
		Assert.notNull(attributes, () ->
				"The '@EnableAmqp' is not present on importing class: " + importMetadata.getClassName());
		this.attributes = attributes;
	}

	@Override
	public void setEnvironment(Environment environment) {
		this.environment = environment;
	}

	@Bean
	@Conditional(OnMissingClientBeanCondition.class)
	Client protonClient() {
		ClientOptions options = new ClientOptions();
		if (this.attributes != null) {
			String id = this.attributes.getString("clientId");
			id = this.environment.resolvePlaceholders(id);
			if (StringUtils.hasText(id)) {
				options.id(id);
			}
			String futureType = this.attributes.getString("clientFutureType");
			futureType = this.environment.resolvePlaceholders(futureType);
			if (StringUtils.hasText(futureType)) {
				options.futureType(futureType);
			}
		}
		return Client.create(options);
	}

	private static final class OnMissingClientBeanCondition implements Condition {

		OnMissingClientBeanCondition() {
		}

		@Override
		@SuppressWarnings("NullAway")
		public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
			return context.getBeanFactory().getBeanNamesForType(Client.class).length == 0;
		}

	}

}
