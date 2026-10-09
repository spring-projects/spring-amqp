/*
 * Copyright 2023-present the original author or authors.
 */

package org.springframework.rabbit.stream.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation.Context;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;

/**
 * Spring RabbitMQ Observation for
 * {@link org.springframework.rabbit.stream.producer.RabbitStreamTemplate}.
 *
 * @author Gary Russell
 * @since 3.0.5
 *
 */
public enum RabbitStreamTemplateObservation implements ObservationDocumentation {

	/**
	 * Observation for {@link RabbitStreamTemplate}s.
	 */
	STREAM_TEMPLATE_OBSERVATION {

		@Override
		public Class<? extends ObservationConvention<? extends Context>> getDefaultConvention() {
			return DefaultRabbitStreamTemplateObservationConvention.class;
		}

		@Override
		public String getPrefix() {
			return "spring.rabbit.stream.template";
		}

		@Override
		public KeyName[] getLowCardinalityKeyNames() {
			return TemplateLowCardinalityTags.values();
		}

	};

	/**
	 * Low cardinality tags.
	 */
	public enum TemplateLowCardinalityTags implements KeyName {

		/**
		 * Bean name of the template.
		 */
		BEAN_NAME {

			@Override
			public String asString() {
				return "spring.rabbit.stream.template.name";
			}

		}

	}

	/**
	 * Default {@link RabbitStreamTemplateObservationConvention} for Rabbit template key values.
	 */
	public static class DefaultRabbitStreamTemplateObservationConvention
			implements RabbitStreamTemplateObservationConvention {

		/**
		 * A singleton instance of the convention.
		 */
		public static final DefaultRabbitStreamTemplateObservationConvention INSTANCE =
				new DefaultRabbitStreamTemplateObservationConvention();

		@Override
		public KeyValues getLowCardinalityKeyValues(RabbitStreamMessageSenderContext context) {
			return KeyValues.of(RabbitStreamTemplateObservation.TemplateLowCardinalityTags.BEAN_NAME.asString(),
							context.getBeanName());
		}

		@Override
		public String getContextualName(RabbitStreamMessageSenderContext context) {
			return context.getDestination() + " send";
		}

	}

}
