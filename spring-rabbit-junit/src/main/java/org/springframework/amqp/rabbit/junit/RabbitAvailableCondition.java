/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;

import com.rabbitmq.client.ConnectionFactory;

/**
 * JUnit5 {@link ExecutionCondition}.
 * Looks for {@code @RabbitAvailable} annotated classes and disables
 * if found the broker is not available.
 *
 * @author Gary Russell
 * @since 2.0.2
 *
 */
public class RabbitAvailableCondition implements ExecutionCondition, AfterAllCallback, ParameterResolver {

	private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled(
			"@RabbitAvailable is not present");

	private static final ThreadLocal<BrokerRunning> brokerRunningHolder = new ThreadLocal<>();

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<AnnotatedElement> element = context.getElement();
		RabbitAvailable rabbit = AnnotationUtils.findAnnotation(element.get(), RabbitAvailable.class);
		if (rabbit != null) {
			try {
				String[] queues = rabbit.queues();
				BrokerRunning brokerRunning = getStore(context).get("brokerRunning", BrokerRunning.class);
				if (brokerRunning == null) {
					if (rabbit.management()) {
						brokerRunning = BrokerRunning.isBrokerAndManagementRunningWithEmptyQueues(queues);
					}
					else {
						brokerRunning = BrokerRunning.isRunningWithEmptyQueues(queues);
					}
				}
				brokerRunning.isUp();
				brokerRunningHolder.set(brokerRunning);
				Store store = getStore(context);
				store.put("brokerRunning", brokerRunning);
				store.put("queuesToDelete", queues);
				return ConditionEvaluationResult.enabled("RabbitMQ is available");
			}
			catch (Exception e) {
				if (BrokerRunning.fatal()) {
					throw new IllegalStateException("Required RabbitMQ is not available");
				}
				return ConditionEvaluationResult.disabled("RabbitMQ is not available");
			}
		}
		return ENABLED;
	}

	@Override
	public void afterAll(ExtensionContext context) throws Exception {
		brokerRunningHolder.remove();
		Store store = getStore(context);
		BrokerRunning brokerRunning = store.remove("brokerRunning", BrokerRunning.class);
		if (brokerRunning != null) {
			brokerRunning.removeTestQueues();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		Class<?> type = parameterContext.getParameter().getType();
		return type.equals(ConnectionFactory.class) || type.equals(BrokerRunning.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
			throws ParameterResolutionException {
		// in parent for method injection, Composite key causes a store miss
		BrokerRunning brokerRunning =
				getParentStore(context).get("brokerRunning", BrokerRunning.class) == null
					? getStore(context).get("brokerRunning", BrokerRunning.class)
					: getParentStore(context).get("brokerRunning", BrokerRunning.class);
		Assert.state(brokerRunning != null, "Could not find brokerRunning instance");
		Class<?> type = parameterContext.getParameter().getType();
		return type.equals(ConnectionFactory.class) ? brokerRunning.getConnectionFactory()
				: brokerRunning;
	}

	private Store getStore(ExtensionContext context) {
		return context.getStore(Namespace.create(getClass(), context));
	}

	private Store getParentStore(ExtensionContext context) {
		ExtensionContext parent = context.getParent().get();
		return parent.getStore(Namespace.create(getClass(), parent));
	}

	public static BrokerRunning getBrokerRunning() {
		return brokerRunningHolder.get();
	}

}
