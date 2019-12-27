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

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;

/**
 * A post processor that uses a {@link DeflaterOutputStream} to compress the message body.
 * Sets {@link org.springframework.amqp.core.MessageProperties#SPRING_AUTO_DECOMPRESS} to
 * true by default.
 *
 * @author David Diehl
 * @since 2.2
 */
public class DeflaterPostProcessor extends AbstractDeflaterPostProcessor {

	public DeflaterPostProcessor() {
	}

	public DeflaterPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	@Override
	protected OutputStream getCompressorStream(OutputStream zipped) throws IOException {
		return new DeflaterPostProcessor.SettableLevelDeflaterOutputStream(zipped, getLevel());
	}

	@Override
	protected String getEncoding() {
		return "deflate";
	}

	private static final class SettableLevelDeflaterOutputStream extends DeflaterOutputStream {

		SettableLevelDeflaterOutputStream(OutputStream out, int level) {
			super(out);
			this.def.setLevel(level);
		}

	}
}
