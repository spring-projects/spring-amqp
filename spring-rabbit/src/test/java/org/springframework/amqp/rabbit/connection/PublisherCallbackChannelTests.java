/*
 * Copyright 2019 the original author or authors.
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

package org.springframework.amqp.rabbit.connection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.willAnswer;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * @author Gary Russell
 * @since 2.1.5
 *
 */
public class PublisherCallbackChannelTests {

	@Test
	public void shutdownWhileCreate() throws IOException, TimeoutException {
		Channel delegate = mock(Channel.class);
		AtomicBoolean npe = new AtomicBoolean();
		willAnswer(inv -> {
			ShutdownListener sdl = inv.getArgument(0);
			try {
				sdl.shutdownCompleted(new ShutdownSignalException(true, false, mock(Method.class), null));
			}
			catch (@SuppressWarnings("unused") NullPointerException e) {
				npe.set(true);
			}
			return null;
		}).given(delegate).addShutdownListener(any());
		PublisherCallbackChannelImpl channel = new PublisherCallbackChannelImpl(delegate, mock(ExecutorService.class));
		assertThat(npe.get()).isFalse();
		channel.close();
	}

}
