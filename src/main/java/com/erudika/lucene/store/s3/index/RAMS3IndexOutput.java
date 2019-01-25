/*
 * Copyright 2004-2019 the original author or authors.
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
package com.erudika.lucene.store.s3.index;

import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import java.io.InputStream;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An <code>IndexOutput</code> implementation that initially writes the data to a memory buffer. Once it exceeds the
 * configured threshold ( {@link #INDEX_OUTPUT_THRESHOLD_SETTING}, will start working with a temporary file, releasing
 * the previous buffer.
 *
 * @author kimchy
 */
public class RAMS3IndexOutput extends IndexOutput implements S3IndexConfigurable {


	private RAMIndexOutput ramIndexOutput;
	private final Checksum crc;

	public RAMS3IndexOutput() {
		super("RAMAndFileS3IndexOutput", "RAMAndFileS3IndexOutput");
		crc = new BufferedChecksum(new CRC32());
	}

	@Override
	public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
			throws IOException {
		ramIndexOutput = new RAMIndexOutput();
		ramIndexOutput.configure(name, s3Directory, settings);
	}

	@Override
	public void writeByte(final byte b) throws IOException {
		ramIndexOutput.writeByte(b);
		crc.update(b);
	}

	@Override
	public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
		ramIndexOutput.writeBytes(b, offset, length);
		crc.update(b, offset, length);
	}

	@Override
	public void close() throws IOException {
		ramIndexOutput.close();
	}

	@Override
	public long getFilePointer() {
		return ramIndexOutput.getFilePointer();
	}

	@Override
	public long getChecksum() throws IOException {
		return crc.getValue();
	}

	/**
	 * An <code>IndexOutput</code> implemenation that stores all the data written to it in memory, and flushes it to the
	 * database when the output is closed.
	 * <p/>
	 * Useful for small file entries like the segment file.
	 *
	 * @author kimchy
	 */
	class RAMIndexOutput extends AbstractS3IndexOutput {

		private final Logger logger = LoggerFactory.getLogger(RAMS3IndexOutput.class);

		public RAMIndexOutput() {
			super("RAMS3IndexOutput");
		}

		private class RAMFile {

			ArrayList<byte[]> buffers = new ArrayList<byte[]>();
			long length;
		}

		private class RAMInputStream extends InputStream {

			private long position;

			private int buffer;

			private int bufferPos;

			private long markedPosition;

			@Override
			public synchronized void reset() throws IOException {
				position = markedPosition;
			}

			@Override
			public boolean markSupported() {
				return true;
			}

			@Override
			public void mark(final int readlimit) {
				markedPosition = position;
			}

			@Override
			public int read(final byte[] dest, int destOffset, final int len) throws IOException {
				if (position == file.length) {
					return -1;
				}
				int remainder = (int) (position + len > file.length ? file.length - position : len);
				final long oldPosition = position;
				while (remainder != 0) {
					if (bufferPos == bufferSize) {
						bufferPos = 0;
						buffer++;
					}
					int bytesToCopy = bufferSize - bufferPos;
					bytesToCopy = bytesToCopy >= remainder ? remainder : bytesToCopy;
					final byte[] buf = file.buffers.get(buffer);
					System.arraycopy(buf, bufferPos, dest, destOffset, bytesToCopy);
					destOffset += bytesToCopy;
					position += bytesToCopy;
					bufferPos += bytesToCopy;
					remainder -= bytesToCopy;
				}
				return (int) (position - oldPosition);
			}

			@Override
			public int read() throws IOException {
				if (position == file.length) {
					return -1;
				}
				if (bufferPos == bufferSize) {
					bufferPos = 0;
					buffer++;
				}
				final byte[] buf = file.buffers.get(buffer);
				position++;
				return buf[bufferPos++] & 0xFF;
			}
		}

		private RAMFile file;

		private int pointer = 0;

		@Override
		public void configure(final String name, final S3Directory s3Directory, final S3FileEntrySettings settings)
				throws IOException {
			super.configure(name, s3Directory, settings);
			file = new RAMFile();
			this.name = name;
			this.s3Directory = s3Directory;
		}

		@Override
		public void flushBuffer(final byte[] src, final int offset, final int len) {
			byte[] buffer;
			int bufferPos = offset;
			while (bufferPos != len) {
				final int bufferNumber = pointer / bufferSize;
				final int bufferOffset = pointer % bufferSize;
				final int bytesInBuffer = bufferSize - bufferOffset;
				final int remainInSrcBuffer = len - bufferPos;
				final int bytesToCopy = bytesInBuffer >= remainInSrcBuffer ? remainInSrcBuffer : bytesInBuffer;

				if (bufferNumber == file.buffers.size()) {
					buffer = new byte[bufferSize];
					file.buffers.add(buffer);
				} else {
					buffer = file.buffers.get(bufferNumber);
				}

				System.arraycopy(src, bufferPos, buffer, bufferOffset, bytesToCopy);
				bufferPos += bytesToCopy;
				pointer += bytesToCopy;
			}

			if (pointer > file.length) {
				file.length = pointer;
			}
		}

		@Override
		protected InputStream openInputStream() throws IOException {
			return new RAMInputStream();
		}

		@Override
		protected void doAfterClose() throws IOException {
			file = null;
		}

		@Override
		public void seek(final long pos) throws IOException {
			super.seek(pos);
			pointer = (int) pos;
		}

		@Override
		public long length() {
			return file.length;
		}

		public void flushToIndexOutput(final IndexOutput indexOutput) throws IOException {
			super.flush();
			if (file.buffers.isEmpty()) {
				return;
			}
			if (file.buffers.size() == 1) {
				indexOutput.writeBytes(file.buffers.get(0), (int) file.length);
				return;
			}
			final int tempSize = file.buffers.size() - 1;
			int i;
			for (i = 0; i < tempSize; i++) {
				indexOutput.writeBytes(file.buffers.get(i), bufferSize);
			}
			final int leftOver = (int) (file.length % bufferSize);
			if (leftOver == 0) {
				indexOutput.writeBytes(file.buffers.get(i), bufferSize);
			} else {
				indexOutput.writeBytes(file.buffers.get(i), leftOver);
			}
		}

		@Override
		public long getChecksum() throws IOException {
			// TODO Auto-generated method stub
			logger.debug("RAMS3IndexOutput.getChecksum()");
			return 0;
		}
	}

}
