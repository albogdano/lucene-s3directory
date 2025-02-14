# lucene-s3directory

This is a Lucene `Directory` implementation for AWS S3. It stores indices in S3 buckets instead of the local file system.
This project is still considered **experimental** but is now in a stable state, meaning, it can be used in production.

There is an open pull request for merge this project into Lucene: apache/lucene#13949.

Also, there's a similar project called [Nixiesearch](https://github.com/nixiesearch) with a broader scope,
aiming to implement a full-featured, cloud-native search engine on top of S3. Check out the [slides by
Roman Grebennikov](https://shuttie.github.io/haystack24-nixiesearch-slides/) for an introduction to Nixiesearch.

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
The `S3Directory` uses legal hold locks on `write.lock` files.

## Getting started

The package is available on Maven Central:
```xml
<dependency>
  <groupId>com.erudika</groupId>
  <artifactId>lucene-s3directory</artifactId>
  <version>1.0.0-SNAPSHOT</version>
</dependency>
```

**Requirements:**

- Java 17+
- Lucene 10+ compatible

To build the project:
```
mvn -DskipTests=true clean install
```

**Usage:**

```java
final S3Directory s3dir = new S3Directory("my-lucene-index");
s3dir.create();

IndexWriterConfig config = new IndexWriterConfig();
config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
config.setUseCompoundFile(false);
try (s3dir; IndexWriter writer = new IndexWriter(s3Dir, config)) {
  Document doc = new Document();
  String word = "lorem ipsum dolor";
  doc.add(new StringField("keyword", word, Field.Store.YES));
  doc.add(new StringField("unindexed", word, Field.Store.YES));
  doc.add(new StringField("unstored", word, Field.Store.NO));
  doc.add(new StringField("text", word, Field.Store.YES));
  writer.addDocument(doc);

  final Query query = new TermQuery(new Term("text", "ipsum"));
  try (DirectoryReader ireader = DirectoryReader.open(s3Dir)) {
    final IndexSearcher isearcher = new IndexSearcher(ireader);
    final TopDocs topDocs = isearcher.search(query, 1000);
    final StoredFields storedFields = isearcher.storedFields();
    final ScoreDoc[] hits = topDocs.scoreDocs;
    // Iterate through the results:
    for (final ScoreDoc hit : hits) {
      final Document hitDoc = storedFields.document(hit.doc);
      System.out.println("This is the text found: " + hitDoc.get("text"));
    }
  } catch (Exception e) {
    e.printStackTrace();
  }
}

// optionally, close or delete manually, if needed.
s3dir.close();
s3dir.delete();
```

The integration tests use [adobe/S3Mock](https://github.com/adobe/S3Mock) library for local testing and don't
require access to the real S3 service nor an AWS account.

## Dependencies

The project initially used the official AWS Java SDK v2, but that dependency was later removed in favor of the excellent
and lightweight [AWS Lightweight Java Client](https://github.com/davidmoten/aws-lightweight-client-java) by @davidmoten.

There are 3 dependencies in total:

- AWS Lightweight Java Client
- Lucene Core
- SLF4J API

## Performance

Performance is not great. Each request to AWS takes a lot of time - TLS handshake, signature calculation, etc.
I tried to do my best to optimize the code but I'm sure it can be optimized further. Contributions are welcome.

`S3DirectoryBenchmarkITest.java`:
```
RAMDirectory Time: 225 ms
FSDirectory Time : 62 ms
S3Directory Time : 16859 ms
```

## Contributions & Goals

Contributions and PRs are welcome, especially those which aim to enhance performance.
The feature I would like to see implemented the most is some sort of block caching for reads, backed by a `MMapDirectory`.
The idea for this feature was presented at [Haystack EU '24 by Roman Grebennikov](https://shuttie.github.io/haystack24-nixiesearch-slides/#/15)
as part of his Nixiesearch presentation.

## License

[Apache 2.0](LICENSE)



