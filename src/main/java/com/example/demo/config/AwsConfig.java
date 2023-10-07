package com.example.demo.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AwsConfig {


    @Bean
    public AmazonKinesis amazonKinesis(){
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("dummy", "dummy");
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        return AmazonKinesisClientBuilder
                .standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                .build();
    }

    @Bean
    public AmazonDynamoDB dynamoDBClientAsync() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("dummy", "dummy");
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCreds);

        return AmazonDynamoDBClientBuilder.standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                .build();
    }

    @Bean
    public AmazonCloudWatch cloudWatch() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials("dummy", "dummy");
        AWSStaticCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCreds);

        return AmazonCloudWatchClientBuilder
                .standard()
                .withCredentials(awsStaticCredentialsProvider)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", "us-west-2"))
                .build();
    }
}
