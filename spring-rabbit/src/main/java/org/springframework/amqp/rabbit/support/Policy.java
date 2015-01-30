/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.amqp.rabbit.support;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a broker policy.
 *
 * @author Gary Russell
 * @since 1.5
 *
 */
public class Policy {

	private final String vhost;

	private final String name;

	private final String pattern;

	private final String applyTo;

	private final Integer priority;

	private final Map<String, Object> definition;

	public Policy(String vhost, String name, String pattern, String applyTo, Integer priority, Map<String, Object> definition) {
		this.vhost = vhost;
		this.name = name;
		this.pattern = pattern;
		this.applyTo = applyTo;
		this.priority = priority;
		this.definition = definition;
	}

	public String getVhost() {
		return vhost;
	}

	public String getName() {
		return name;
	}

	public String getPattern() {
		return pattern;
	}

	@JsonProperty("apply-to")
	@JsonInclude(Include.NON_NULL)
	public String getApplyTo() {
		return applyTo;
	}

	@JsonInclude(Include.NON_NULL)
	public Integer getPriority() {
		return priority;
	}

	public Map<String, Object> getDefinition() {
		return definition;
	}

	@Override
	public String toString() {
		return "Policy [vhost=" + vhost + ", name=" + name + ", pattern=" + pattern + ", applyTo=" + applyTo
				+ ", priority=" + priority + ", definition=" + definition + "]";
	}

}
