package com.erudika.lucene.store.s3;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.HashMap;

public class S3FileSystemStore {
    private static FileSystem fileSystem;
    private static FileSystem fileSystem2;

    public static FileSystem getS3FileSystem() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystems.newFileSystem(URI.create("s3://lucene-test"), new HashMap<String, Object>());
        }
        return fileSystem;
    }

    public static FileSystem getTaxonomyS3FileSystem() throws IOException {
        if (fileSystem2 == null) {
            fileSystem2 = FileSystems.newFileSystem(URI.create("s3://avinash-test-1"), new HashMap<String, Object>());
        }
        return fileSystem2;
    }
}
