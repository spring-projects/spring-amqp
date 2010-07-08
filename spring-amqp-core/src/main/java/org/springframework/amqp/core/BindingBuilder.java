/*
 * Copyright 2002-2010 the original author or authors.
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


/**
 * Basic builder class to create bindings for a more fluent API style in code based configuration.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public final class BindingBuilder  {

	private Queue queue;

	private AbstractExchange exchange;

	private String routingKey;

	//return intermediate object that only allows to call 'to' to configure the binding.
	public static BindingBuilder from(Queue queue) {
		return new BindingBuilder(queue);
	}


	private BindingBuilder(Queue queue) {
		super();
		this.queue = queue;
	}

	//return intermediate object that only allows to call 'with' to configure the binding.
	public BindingBuilder to(AbstractExchange exchange) {
		this.exchange = exchange;
		return this;
	}

	// creates the product.
	public Binding with(String routingKey) {
		this.routingKey = routingKey;		
		return new Binding(this.queue, this.exchange, this.routingKey);
	}

	@SuppressWarnings("unchecked")
	public Binding with(Enum routingKeyEnum) {
		this.routingKey = routingKeyEnum.toString();
		return new Binding(this.queue, this.exchange, this.routingKey);
	}

}
