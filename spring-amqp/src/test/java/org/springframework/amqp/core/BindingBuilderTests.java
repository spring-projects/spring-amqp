/*
 * Copyright 2002-2016 the original author or authors.
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

package org.springframework.amqp.core;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import org.junit.Test;

/**
 * @author Mark Fisher
 */
public class BindingBuilderTests {

	@Test
	public void fanoutBinding() {
		Binding binding = BindingBuilder.bind(new Queue("q")).to(new FanoutExchange("f"));
		assertNotNull(binding);
	}

	@Test
	public void directBinding() {
		Binding binding = BindingBuilder.bind(new Queue("q")).to(new DirectExchange("d")).with("r");
		assertNotNull(binding);
	}

	@Test
	public void directBindingWithQueueName() {
		Binding binding = BindingBuilder.bind(new Queue("q")).to(new DirectExchange("d")).withQueueName();
		assertNotNull(binding);
	}

	@Test
	public void topicBinding() {
		Binding binding = BindingBuilder.bind(new Queue("q")).to(new TopicExchange("t")).with("r");
		assertNotNull(binding);
	}

	@Test
	public void customBinding() {
		class CustomExchange extends AbstractExchange {
			CustomExchange(String name) {
				super(name);
			}

			@Override
			public String getType() {
				return "x-custom";
			}
		}
		Binding binding = BindingBuilder.//
				bind(new Queue("q")).//
				to(new CustomExchange("f")).//
				with("r").//
				and(Collections.<String, Object>singletonMap("k", new Object()));
		assertNotNull(binding);
	}

	@Test
	public void exchangeBinding() {
		Binding binding = BindingBuilder.bind(new DirectExchange("q")).to(new FanoutExchange("f"));
		assertNotNull(binding);
	}

}
