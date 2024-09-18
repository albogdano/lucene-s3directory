# lucene-s3directory

:warning: **EXPERIMENTAL** :warning:

This is a Lucene `Directory` implementation for AWS S3. It stores indices in S3 buckets instead of the local file system.
This is just a proof of concept for now and is **not** suitable for production use.

## Motivation

The project was inspired by Shay Banon (kimchy), creator of [Elasticsearch](https://github.com/elastic/elasticsearch)
and [Compass](http://www.compass-project.org/). It is a direct fork of his `JdbcDirectory` which is part of Compass.

Back in 2007, Shay wrote about the idea of Lucene-to-S3 integration in his
[blog post](https://github.com/kimchy/kimchy.github.com/blob/master/_posts/2007-11-16-lucene-and-amazon-s3.textile):

> I spent some time trying to have the ability to store Lucene index on Amazon S3 service. Amazon S3 is a really cool
> idea, and having the ability to store Lucene index on top of it will provide a simple way to allow storing Lucene
> index in a distributed environment supporting HA. It will also make a lot of sense for applications deployed on
> Amazon EC2, since working with S3 from EC2 is free.

But back then S3 did not support locking so he scrapped the implementation:

> It would be great if the good people at Amazon would allow for simple locking support. I understand that this is not
> simple to do in a distributed environment, but it must be there in some form, it will make S3 much a more attractive offer.

Since late 2018 [S3 supports locking](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html).
The `S3Directory` uses legal hold locks on `write.lock` files. The AWS Java SDK v2.0 is used for that reason.

## Getting started

**Requirements:**

- Java 1.8+
- Lucene 7.6+ compatible

To build the project:
```
mvn -DskipTests=true clean install
```

**Usage:**

```java
S3Directory dir = new S3Directory("my-lucene-index");
dir.create();

// use it in your code in place of FSDirectory, for example

// finally
dir.close();
dir.delete();
```

To run the integration tests, you'll need to have a valid AWS profile configured on your system. The tests will
run against the real S3 service on AWS.

## Performance

Performance is not great. Each request to AWS takes a lot of time - TLS handshake, signature calculation, etc.
I tried to do my best to optimize the code but I'm sure it can be optimized further. Contributions are welcome.

`S3DirectoryBenchmarkITest.java`:
```
RAMDirectory Time: 225 ms
FSDirectory Time : 62 ms
S3Directory Time : 16859 ms
```

## License

[Apache 2.0](LICENSE)



