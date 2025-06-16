/*
 * Copyright 2017-present the original author or authors.
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

package org.springframework.amqp.rabbit.junit;

import java.lang.reflect.AnnotatedElement;
import java.util.Optional;

import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.util.Assert;

/**
 * JUnit5 {@link ExecutionCondition}. Looks for {@code @RabbitAvailable} annotated classes
 * and disables if found the broker is not available.
 *
 * @author Gary Russell
 * @since 2.0.2
 *
 */
public class RabbitAvailableCondition
		implements ExecutionCondition, AfterEachCallback, AfterAllCallback, ParameterResolver {

	private static final String BROKER_RUNNING_BEAN = "brokerRunning";

	private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled(
			"@RabbitAvailable is not present");

	private static final ThreadLocal<BrokerRunningSupport> BROKER_RUNNING_HOLDER = new ThreadLocal<>();

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<AnnotatedElement> element = context.getElement();
		MergedAnnotations annotations = MergedAnnotations.from(element.get(),
				MergedAnnotations.SearchStrategy.TYPE_HIERARCHY);
		if (annotations.get(RabbitAvailable.class).isPresent()) {
			RabbitAvailable rabbit = annotations.get(RabbitAvailable.class).synthesize();
			try {
				String[] queues = rabbit.queues();
				BrokerRunningSupport brokerRunning = getStore(context).get(BROKER_RUNNING_BEAN,
						BrokerRunningSupport.class);
				if (brokerRunning == null) {
					if (rabbit.management()) {
						brokerRunning = BrokerRunningSupport.isBrokerAndManagementRunningWithEmptyQueues(queues);
					}
					else {
						brokerRunning = BrokerRunningSupport.isRunningWithEmptyQueues(queues);
					}
				}
				brokerRunning.setPurgeAfterEach(rabbit.purgeAfterEach());
				brokerRunning.test();
				BROKER_RUNNING_HOLDER.set(brokerRunning);
				Store store = getStore(context);
				store.put(BROKER_RUNNING_BEAN, brokerRunning);
				store.put("queuesToDelete", queues);
				return ConditionEvaluationResult.enabled("RabbitMQ is available");
			}
			catch (Exception e) {
				if (BrokerRunningSupport.fatal()) {
					throw new IllegalStateException("Required RabbitMQ is not available", e);
				}
				return ConditionEvaluationResult.disabled("Tests Ignored: RabbitMQ is not available");
			}
		}
		return ENABLED;
	}

	@Override
	public void afterEach(ExtensionContext context) {
		BrokerRunningSupport brokerRunning = BROKER_RUNNING_HOLDER.get();
		if (brokerRunning != null && brokerRunning.isPurgeAfterEach()) {
			brokerRunning.purgeTestQueues();
		}
	}

	@Override
	public void afterAll(ExtensionContext context) {
		BROKER_RUNNING_HOLDER.remove();
		Store store = getStore(context);
		BrokerRunningSupport brokerRunning = store.remove(BROKER_RUNNING_BEAN, BrokerRunningSupport.class);
		if (brokerRunning != null) {
			brokerRunning.removeTestQueues();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		Class<?> type = parameterContext.getParameter().getType();
		return type.equals(ConnectionFactory.class) || type.equals(BrokerRunningSupport.class);
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext context)
			throws ParameterResolutionException {
		// in parent for method injection, Composite key causes a store miss
		BrokerRunningSupport brokerRunning = getParentStore(context).get(BROKER_RUNNING_BEAN,
				BrokerRunningSupport.class) == null
						? getStore(context).get(BROKER_RUNNING_BEAN, BrokerRunningSupport.class)
						: getParentStore(context).get(BROKER_RUNNING_BEAN, BrokerRunningSupport.class);
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

	public static BrokerRunningSupport getBrokerRunning() {
		return BROKER_RUNNING_HOLDER.get();
	}

}
