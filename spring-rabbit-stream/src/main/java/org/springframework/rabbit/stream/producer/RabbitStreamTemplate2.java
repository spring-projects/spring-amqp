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

package org.springframework.rabbit.stream.producer;

import java.util.concurrent.CompletableFuture;

import com.rabbitmq.stream.Environment;

/**
 * This interface was added in 2.4.7 to aid migration from methods returning
 * {@code ListenableFuture}s to {@link CompletableFuture}s.
 *
 * @author Gary Russell
 * @since 2.8
 * @deprecated in favor of {@link RabbitStreamTemplate}.
 */
@Deprecated
public class RabbitStreamTemplate2 extends RabbitStreamTemplate implements RabbitStreamOperations2 {

	public RabbitStreamTemplate2(Environment environment, String streamName) {
		super(environment, streamName);
	}

}
