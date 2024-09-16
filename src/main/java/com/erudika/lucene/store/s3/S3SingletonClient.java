package com.erudika.lucene.store.s3;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class S3SingletonClient {
    private static S3Client s3Client;

    private S3SingletonClient(){

    }

    public static S3Client getS3Client(){
        if(s3Client == null){

            String accessKey = System.getenv("aws_access_key_id");
            String secretKey = System.getenv("aws_secret_access_key");
            String sessionToken = System.getenv("aws_session_token");
            s3Client = S3Client.builder()
                    .credentialsProvider(() -> new AwsSessionCredentials
                            .Builder()
                            .accessKeyId(accessKey) //This is from E2E account
                            .secretAccessKey(secretKey)
                            .sessionToken(sessionToken)
                            .build()
                    )
                    .region(Region.US_WEST_2)
                    .build();
        }
        return s3Client;
    }
}
