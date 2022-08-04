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

package org.springframework.amqp.rabbit.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.reflect.Method;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.amqp.utils.test.TestUtils;

/**
 * @author Gary Russell
 * @since 3.0
 *
 */
public class BatchMessagingMessageListenerAdapterTests {

	@Test
	void compatibleMethod() throws Exception {
		Method method = getClass().getDeclaredMethod("listen", List.class);
		BatchMessagingMessageListenerAdapter adapter = new BatchMessagingMessageListenerAdapter(this, method, false,
				null, null);
		assertThat(TestUtils.getPropertyValue(adapter, "messagingMessageConverter.inferredArgumentType"))
				.isEqualTo(String.class);
		Method badMethod = getClass().getDeclaredMethod("listen", String.class);
		assertThatIllegalStateException().isThrownBy(() ->
				new BatchMessagingMessageListenerAdapter(this, badMethod, false, null, null)
			).withMessageStartingWith("Mis-configuration");
	}

	public void listen(String in) {
	}

	public void listen(List<String> in) {
	}

}
