package com.erudika.lucene.store.s3.index;

import java.io.IOException;

import org.apache.lucene.store.IndexOutput;

/**
 * A simple base class that performs index output memory based buffering. The buffer size if configurable.
 *
 * @author kimchy
 */
// NEED TO BE MONITORED AGAINST LUCENE
public abstract class ConfigurableBufferedIndexOutput extends IndexOutput {

	public static final int DEFAULT_BUFFER_SIZE = 16384;

	private byte[] buffer;
	private long bufferStart = 0; // position in file of buffer
	private int bufferPosition = 0; // position in buffer

	protected int bufferSize = DEFAULT_BUFFER_SIZE;

	protected ConfigurableBufferedIndexOutput(final String resourceDescription) {
		super(resourceDescription, "ConfigurableBufferedIndexOutput");
	}

	protected void initBuffer(final int bufferSize) {
		this.bufferSize = bufferSize;
		buffer = new byte[bufferSize];
	}

	/**
	 * Writes a single byte.
	 *
	 * @see org.apache.lucene.store.IndexInput#readByte()
	 */
	@Override
	public void writeByte(final byte b) throws IOException {
		if (bufferPosition >= bufferSize) {
			flush();
		}
		buffer[bufferPosition++] = b;
	}

	/**
	 * Writes an array of bytes.
	 *
	 * @param b the bytes to write
	 * @param length the number of bytes to write
	 * @see org.apache.lucene.store.IndexInput#readBytes(byte[],int,int)
	 */
	@Override
	public void writeBytes(final byte[] b, final int offset, final int length) throws IOException {
		int bytesLeft = bufferSize - bufferPosition;
		// is there enough space in the buffer?
		if (bytesLeft >= length) {
			// we add the data to the end of the buffer
			System.arraycopy(b, offset, buffer, bufferPosition, length);
			bufferPosition += length;
			// if the buffer is full, flush it
			if (bufferSize - bufferPosition == 0) {
				flush();
			}
		} else {
			// is data larger then buffer?
			if (length > bufferSize) {
				// we flush the buffer
				if (bufferPosition > 0) {
					flush();
				}
				// and write data at once
				flushBuffer(b, offset, length);
				bufferStart += length;
			} else {
				// we fill/flush the buffer (until the input is written)
				int pos = 0; // position in the input data
				int pieceLength;
				while (pos < length) {
					pieceLength = length - pos < bytesLeft ? length - pos : bytesLeft;
					System.arraycopy(b, pos + offset, buffer, bufferPosition, pieceLength);
					pos += pieceLength;
					bufferPosition += pieceLength;
					// if the buffer is full, flush it
					bytesLeft = bufferSize - bufferPosition;
					if (bytesLeft == 0) {
						flush();
						bytesLeft = bufferSize;
					}
				}
			}
		}
	}

	/**
	 * Forces any buffered output to be written.
	 */
	public void flush() throws IOException {
		flushBuffer(buffer, bufferPosition);
		bufferStart += bufferPosition;
		bufferPosition = 0;
	}

	/**
	 * Expert: implements buffer write. Writes bytes at the current position in the output.
	 *
	 * @param b the bytes to write
	 * @param len the number of bytes to write
	 */
	private void flushBuffer(final byte[] b, final int len) throws IOException {
		flushBuffer(b, 0, len);
	}

	/**
	 * Expert: implements buffer write. Writes bytes at the current position in the output.
	 *
	 * @param b the bytes to write
	 * @param offset the offset in the byte array
	 * @param len the number of bytes to write
	 */
	protected abstract void flushBuffer(byte[] b, int offset, int len) throws IOException;

	/**
	 * Closes this stream to further operations.
	 */
	@Override
	public void close() throws IOException {
		flush();
	}

	/**
	 * Returns the current position in this file, where the next write will occur.
	 *
	 * @see #seek(long)
	 */
	@Override
	public long getFilePointer() {
		return bufferStart + bufferPosition;
	}

	/**
	 * Sets current position in this file, where the next write will occur.
	 *
	 * @see #getFilePointer()
	 */
	public void seek(final long pos) throws IOException {
		flush();
		bufferStart = pos;
	}

	/**
	 * The number of bytes in the file.
	 */
	public abstract long length() throws IOException;

}
