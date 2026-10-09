/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp.rabbit.junit;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

/**
 * @author Ngoc Nhan
 *
 * @since 4.2
 */
public class BrokerRunningSupportTests {

	BrokerRunningSupport support = BrokerRunningSupport.isNotRunning();

	@Test
	void generateIdByDefault() {

		String name = this.support.generateId();
		assertThat(name).startsWith("SpringBrokerRunning.");

		String encoded = name.substring("SpringBrokerRunning.".length());
		assertThat(encoded).doesNotContain("=").hasSize(22);
	}

	@Test
	void generateIdWithMockRandomUuid() {

		UUID uuid = UUID.fromString("fcbfc9b4-d4d6-4cc0-b8b0-dfa245ffaea5");

		try (MockedStatic<UUID> mockedStatic = mockStatic()) {

			mockedStatic.when(UUID::randomUUID).thenReturn(uuid);
			String name = this.support.generateId();
			assertThat(name).isEqualTo("SpringBrokerRunning._L_JtNTWTMC4sN-iRf-upQ");

			String encoded = name.substring("SpringBrokerRunning.".length());
			assertThat(encoded).doesNotContain("=").hasSize(22);

			byte[] bytes = Base64.getUrlDecoder().decode(encoded);
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			assertThat(new UUID(bb.getLong(), bb.getLong()).toString())
					.isEqualTo("fcbfc9b4-d4d6-4cc0-b8b0-dfa245ffaea5");
		}
	}

}
