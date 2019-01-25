package com.erudika.lucene.store.s3.index;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;

/**
 * A simple base class that performs index input memory based buffering. Allows the buffer size to be configurable.
 *
 * @author kimchy
 */
// NEED TO BE MONITORED AGAINST LUCENE (EXATCLY THE SAME)
public abstract class ConfigurableBufferedIndexInput extends IndexInput {

	protected ConfigurableBufferedIndexInput(final String resourceDescription, final int bufferSize) {
		super(resourceDescription);
		checkBufferSize(bufferSize);
		this.bufferSize = bufferSize;
	}

	/**
	 * Default buffer size
	 */
	public static final int BUFFER_SIZE = 1024;

	protected int bufferSize = BUFFER_SIZE;

	protected byte[] buffer;

	protected long bufferStart = 0; // position in file of buffer
	protected int bufferLength = 0; // end of valid bytes
	protected int bufferPosition = 0; // next byte to read

	@Override
	public byte readByte() throws IOException {
		if (bufferPosition >= bufferLength) {
			refill();
		}
		return buffer[bufferPosition++];
	}

	/**
	 * Change the buffer size used by this IndexInput
	 */
	public void setBufferSize(final int newSize) {
		assert buffer == null || bufferSize == buffer.length;
		if (newSize != bufferSize) {
			checkBufferSize(newSize);
			bufferSize = newSize;
			if (buffer != null) {
				// Resize the existing buffer and carefully save as
				// many bytes as possible starting from the current
				// bufferPosition
				final byte[] newBuffer = new byte[newSize];
				final int leftInBuffer = bufferLength - bufferPosition;
				final int numToCopy;
				if (leftInBuffer > newSize) {
					numToCopy = newSize;
				} else {
					numToCopy = leftInBuffer;
				}
				System.arraycopy(buffer, bufferPosition, newBuffer, 0, numToCopy);
				bufferStart += bufferPosition;
				bufferPosition = 0;
				bufferLength = numToCopy;
				buffer = newBuffer;
			}
		}
	}

	/**
	 * Returns buffer size. @see #setBufferSize
	 */
	public int getBufferSize() {
		return bufferSize;
	}

	private void checkBufferSize(final int bufferSize) {
		if (bufferSize <= 0) {
			throw new IllegalArgumentException("bufferSize must be greater than 0 (got " + bufferSize + ")");
		}
	}

	@Override
	public void readBytes(final byte[] b, int offset, int len) throws IOException {
		if (len <= bufferLength - bufferPosition) {
			// the buffer contains enough data to satistfy this request
			if (len > 0) {
				System.arraycopy(buffer, bufferPosition, b, offset, len);
			}
			bufferPosition += len;
		} else {
			// the buffer does not have enough data. First serve all we've got.
			final int available = bufferLength - bufferPosition;
			if (available > 0) {
				System.arraycopy(buffer, bufferPosition, b, offset, available);
				offset += available;
				len -= available;
				bufferPosition += available;
			}
			// and now, read the remaining 'len' bytes:
			if (len < bufferSize) {
				// If the amount left to read is small enough, do it in the
				// usual
				// buffered way: fill the buffer and copy from it:
				refill();
				if (bufferLength < len) {
					// Throw an exception when refill() could not read len
					// bytes:
					System.arraycopy(buffer, 0, b, offset, bufferLength);
					throw new IOException("read past EOF");
				} else {
					System.arraycopy(buffer, 0, b, offset, len);
					bufferPosition = len;
				}
			} else {
				// The amount left to read is larger than the buffer - there's
				// no
				// performance reason not to read it all at once. Note that
				// unlike
				// the previous code of this function, there is no need to do a
				// seek
				// here, because there's no need to reread what we had in the
				// buffer.
				final long after = bufferStart + bufferPosition + len;
				if (after > length()) {
					throw new IOException("read past EOF");
				}
				readInternal(b, offset, len);
				bufferStart = after;
				bufferPosition = 0;
				bufferLength = 0; // trigger refill() on read
			}
		}
	}

	protected void refill() throws IOException {
		final long start = bufferStart + bufferPosition;
		long end = start + bufferSize;
		if (end > length()) {
			end = length();
		}
		bufferLength = (int) (end - start);
		if (bufferLength <= 0) {
			throw new IOException("read past EOF");
		}

		if (buffer == null) {
			buffer = new byte[bufferSize]; // allocate buffer lazily
			seekInternal(bufferStart);
		}
		readInternal(buffer, 0, bufferLength);

		bufferStart = start;
		bufferPosition = 0;
	}

	/**
	 * Expert: implements buffer refill. Reads bytes from the current position in the input.
	 *
	 * @param b the array to read bytes into
	 * @param offset the offset in the array to start storing bytes
	 * @param length the number of bytes to read
	 */
	protected abstract void readInternal(byte[] b, int offset, int length) throws IOException;

	@Override
	public long getFilePointer() {
		return bufferStart + bufferPosition;
	}

	@Override
	public void seek(final long pos) throws IOException {
		if (pos >= bufferStart && pos < bufferStart + bufferLength) {
			bufferPosition = (int) (pos - bufferStart); // seek within buffer
		} else {
			bufferStart = pos;
			bufferPosition = 0;
			bufferLength = 0; // trigger refill() on read()
			seekInternal(pos);
		}
	}

	/**
	 * Expert: implements seek. Sets current position in this file, where the next {@link #readInternal(byte[],int,int)}
	 * will occur.
	 *
	 * @see #readInternal(byte[],int,int)
	 */
	protected abstract void seekInternal(long pos) throws IOException;

	@Override
	public IndexInput clone() {
		final ConfigurableBufferedIndexInput clone = (ConfigurableBufferedIndexInput) super.clone();

		clone.buffer = null;
		clone.bufferLength = 0;
		clone.bufferPosition = 0;
		clone.bufferStart = getFilePointer();

		return clone;
	}

}
