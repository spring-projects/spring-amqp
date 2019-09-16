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
import java.io.InputStream;
import java.util.zip.InflaterInputStream;

/**
 * A post processor that uses a {@link InflaterInputStream} to decompress the
 * message body.
 *
 * @author David Diehl
 * @since 2.2
 */
public class InflaterPostProcessor extends AbstractDecompressingPostProcessor {
	public InflaterPostProcessor() {
	}

	public InflaterPostProcessor(boolean alwaysDecompress) {
		super(alwaysDecompress);
	}

	protected InputStream getDecompressorStream(InputStream zipped) throws IOException {
		return new InflaterInputStream(zipped);
	}

	protected String getEncoding() {
		return "deflate";
	}
}
