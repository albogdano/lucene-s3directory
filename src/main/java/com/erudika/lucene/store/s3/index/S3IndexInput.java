package com.erudika.lucene.store.s3.index;

import com.erudika.lucene.store.s3.S3Directory;
import com.erudika.lucene.store.s3.S3FileEntrySettings;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class S3IndexInput extends BufferedIndexInput implements S3IndexConfigurable {

    private S3Directory s3Directory;
    private String name;
    private long position = 0;
    private long length = -1;

    private static final Logger logger = LoggerFactory.getLogger(S3IndexInput.class);

    public S3IndexInput() {
        super("S3IndexInput");
    }


    @Override
    public void configure(String name, S3Directory s3Directory, S3FileEntrySettings settings) throws IOException {
        this.s3Directory = s3Directory;
        this.name = name;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        synchronized (this) {
            if (position + b.remaining() > length()) {
                throw new EOFException("read past EOF: " + this);
            }
            ResponseInputStream<GetObjectResponse> res = s3Directory.getS3().getObject(
                    GetObjectRequest.builder()
                            .bucket(s3Directory.getBucket())
                            .key(name).build()
            );

            res.skip(position);
            position += b.remaining();
            byte[] data = res.readAllBytes();
            logger.info("Name: " + name
                    + " Reading from S3 at position: " + position
                    + " with buffer size: " + getBufferSize()
                    + " and remaining: " + b.remaining()
                    + " and data length: " + data.length
            );
            b.put(data, 0, b.remaining());

        }

    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        synchronized (this){
            if(pos < 0) {
                throw new IllegalArgumentException("Seek position cannot be negative");
            }
            if(pos > length()) {
                throw new EOFException("Seek position is past EOF");
            }
            logger.info("Name: " + name + " Seeking to position: " + pos);
            position = pos;
        }

    }

    @Override
    public void close() throws IOException {
        //DO NOTHING
    }

    @Override
    public long length() {
        if(length == -1){
            length = s3Directory.getS3().getObject(
                    GetObjectRequest.builder()
                            .bucket(s3Directory.getBucket())
                            .key(name).build(),
                    ResponseTransformer.toInputStream()
            ).response().contentLength();
        }
        return length;
    }

    /**
     * Implementation of an IndexInput that reads from a portion of a file.
     */
    private static final class SlicedIndexInput extends BufferedIndexInput {

        IndexInput base;
        long fileOffset;
        long length;

        SlicedIndexInput(final String sliceDescription, final IndexInput base, final long offset, final long length) {
            super(sliceDescription == null ? base.toString() : base.toString() + " [slice=" + sliceDescription + "]",
                    BufferedIndexInput.BUFFER_SIZE);
            if (offset < 0 || length < 0 || offset + length > base.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: " + base);
            }
            this.base = base.clone();
            fileOffset = offset;
            this.length = length;
        }

        @Override
        public SlicedIndexInput clone() {
            final SlicedIndexInput clone = (SlicedIndexInput) super.clone();
            clone.base = base.clone();
            clone.fileOffset = fileOffset;
            clone.length = length;
            return clone;
        }

        @Override
        protected void readInternal(ByteBuffer bb) throws IOException {
            long start = getFilePointer();
            if (start + bb.remaining() > length) {
                throw new EOFException("read past EOF: " + this);
            }
            base.seek(fileOffset + start);
            base.readBytes(bb.array(), bb.position(), bb.remaining());
            bb.position(bb.position() + bb.remaining());
        }

        @Override
        protected void seekInternal(final long pos) {
        }

        @Override
        public void close() throws IOException {
            base.close();
        }

        @Override
        public long length() {
            return length;
        }
    }

}
