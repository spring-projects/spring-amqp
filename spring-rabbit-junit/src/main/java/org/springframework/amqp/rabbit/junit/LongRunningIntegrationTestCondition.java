/*
 * Copyright 2017-2025 the original author or authors.
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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.util.StringUtils;

/**
 * {@link ExecutionCondition} to skip long-running tests unless an environment
 * variable or property is set.
 *
 * @author Gary Russell
 * @author Artem Bilan
 *
 * @since 2.0.2
 *
 * @see LongRunning
 */
public class LongRunningIntegrationTestCondition implements ExecutionCondition {

	public static final String RUN_LONG_INTEGRATION_TESTS = "RUN_LONG_INTEGRATION_TESTS";

	private static final ConditionEvaluationResult ENABLED = ConditionEvaluationResult.enabled(
			"@LongRunning is not present");

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		Optional<AnnotatedElement> element = context.getElement();
		MergedAnnotations annotations = MergedAnnotations.from(element.get(),
				MergedAnnotations.SearchStrategy.TYPE_HIERARCHY);
		MergedAnnotation<LongRunning> mergedAnnotation = annotations.get(LongRunning.class);
		if (mergedAnnotation.isPresent()) {
			LongRunning longRunning = mergedAnnotation.synthesize();
			String property = longRunning.value();
			if (!StringUtils.hasText(property)) {
				property = RUN_LONG_INTEGRATION_TESTS;
			}
			return JUnitUtils.parseBooleanProperty(property)
					? ConditionEvaluationResult.enabled("Long running tests must run")
					: ConditionEvaluationResult.disabled("Long running tests are skipped");
		}
		return ENABLED;
	}

}
