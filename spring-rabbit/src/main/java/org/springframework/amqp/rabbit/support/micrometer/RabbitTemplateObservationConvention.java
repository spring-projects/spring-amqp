/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.amqp.rabbit.support.micrometer;

import org.springframework.lang.Nullable;

import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.Observation.ObservationConvention;

/**
 * {@link ObservationConvention} for Rabbit template key values. Allows users
 * to add {@link KeyValue}s to the observations.
 *
 * @author Gary Russell
 * @since 3.0
 *
 */
public class RabbitTemplateObservationConvention implements ObservationConvention<RabbitMessageSenderContext> {

	@Nullable
	private final KeyValues lowCardinality;

	@Nullable
	private final KeyValues highCardinality;

	/**
	 * Create an instance with the provided {@link KeyValues}.
	 * @param lowCardinality the low cardinality {@link KeyValues}.
	 * @param highCardinality the high cardinality {@link KeyValues}.
	 */
	public RabbitTemplateObservationConvention(@Nullable KeyValues lowCardinality,
			@Nullable KeyValues highCardinality) {

		this.lowCardinality = lowCardinality != null ? KeyValues.of(lowCardinality) : KeyValues.empty();
		this.highCardinality = highCardinality != null ? KeyValues.of(highCardinality) : KeyValues.empty();
	}

	@Override
	public boolean supportsContext(Context context) {
		return context instanceof RabbitMessageSenderContext;
	}

	@Override
	public KeyValues getLowCardinalityKeyValues(RabbitMessageSenderContext context) {
		return KeyValues.of(this.lowCardinality);
	}

	@Override
	public KeyValues getHighCardinalityKeyValues(RabbitMessageSenderContext context) {
		return this.highCardinality
					.and(RabbitTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.asString(),
							context.getBeanName());
	}

}
