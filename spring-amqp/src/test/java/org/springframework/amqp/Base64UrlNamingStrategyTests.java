/*
 * Copyright 2002-present the original author or authors.
 */

package org.springframework.amqp;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.springframework.amqp.core.Base64UrlNamingStrategy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mockStatic;

/**
 * @author Ngoc Nhan
 *
 * @since 4.2
 */
public class Base64UrlNamingStrategyTests {

	Base64UrlNamingStrategy strategy = new Base64UrlNamingStrategy();

	@Test
	void generateNameByDefault() {

		String name = this.strategy.generateName();
		assertThat(name).startsWith("spring.gen-");

		String encoded = name.substring("spring.gen-".length());
		assertThat(encoded).doesNotContain("=").hasSize(22);
	}

	@Test
	void generateNameWithMockRandomUuid() {

		UUID uuid = UUID.fromString("fcbfc9b4-d4d6-4cc0-b8b0-dfa245ffaea5");

		try (MockedStatic<UUID> mockedStatic = mockStatic()) {

			mockedStatic.when(UUID::randomUUID).thenReturn(uuid);
			String name = this.strategy.generateName();
			assertThat(name).isEqualTo("spring.gen-_L_JtNTWTMC4sN-iRf-upQ");

			String encoded = name.substring("spring.gen-".length());
			assertThat(encoded).doesNotContain("=").hasSize(22);

			byte[] bytes = Base64.getUrlDecoder().decode(encoded);
			ByteBuffer bb = ByteBuffer.wrap(bytes);
			assertThat(new UUID(bb.getLong(), bb.getLong()).toString())
					.isEqualTo("fcbfc9b4-d4d6-4cc0-b8b0-dfa245ffaea5");
		}
	}

}
