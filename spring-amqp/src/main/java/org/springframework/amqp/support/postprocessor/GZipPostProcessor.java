/*
 * Copyright 2014-2016 the original author or authors.
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

package org.springframework.amqp.support.postprocessor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.amqp.core.MessageProperties;

/**
 * A post processor that uses a {@link GZIPOutputStream} to compress the
 * message body. Sets {@link MessageProperties#SPRING_AUTO_DECOMPRESS} to true
 * by default.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class GZipPostProcessor extends AbstractDeflaterPostProcessor {

	public GZipPostProcessor() {
		super();
	}

	public GZipPostProcessor(boolean autoDecompress) {
		super(autoDecompress);
	}

	@Override
	protected OutputStream getCompressorStream(OutputStream zipped) throws IOException {
		return new SettableLevelGZIPOutputStream(zipped, this.level);
	}

	@Override
	protected String getEncoding() {
		return "gzip";
	}

	private static final class SettableLevelGZIPOutputStream extends GZIPOutputStream {

		SettableLevelGZIPOutputStream(OutputStream out, int level) throws IOException {
			super(out);
			this.def.setLevel(level);
		}

	}

}
