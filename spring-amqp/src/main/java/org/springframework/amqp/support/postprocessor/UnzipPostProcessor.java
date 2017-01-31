/*
 * Copyright 2014-2017 the original author or authors.
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
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.springframework.util.Assert;

/**
 * A post processor that uses a {@link ZipInputStream} to decompress the
 * message body.
 *
 * @author Gary Russell
 * @since 1.4.2
 */
public class UnzipPostProcessor extends AbstractDecompressingPostProcessor {

	public UnzipPostProcessor() {
		super();
	}

	public UnzipPostProcessor(boolean alwaysDecompress) {
		super(alwaysDecompress);
	}

	@Override
	protected InputStream getDecompressorStream(InputStream zipped) throws IOException {
		ZipInputStream zipper = new ZipInputStream(zipped);
		ZipEntry entry = zipper.getNextEntry();
		String entryName = entry.getName();
		Assert.state("amqp".equals(entryName), () -> "Zip 'entryName' must be 'amqp', not '" + entryName + "'");
		return zipper;
	}

	@Override
	protected String getEncoding() {
		return "zip";
	}

}
