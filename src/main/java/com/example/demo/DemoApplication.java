package com.example.demo;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.example.demo.config.AmazonKinesisApplicationRecordProcessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.UUID;

@SpringBootApplication
public class DemoApplication {


	private static final String SAMPLE_APPLICATION_NAME = "test-app";
	public static final String SAMPLE_APPLICATION_STREAM_NAME = "test-stream";

	private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
			InitialPositionInStream.LATEST;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}


	@Component
	public class ApplicationRunnerBean implements ApplicationRunner {
		@Override
		public void run(ApplicationArguments arg0) throws Exception {
			System.out.println("ApplicationRunnerBean");


			BasicAWSCredentials awsCredentials = new BasicAWSCredentials("dummy", "dummy");
			AWSCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
			String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

			KinesisClientLibConfiguration kinesisClientLibConfiguration =
					new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
							SAMPLE_APPLICATION_STREAM_NAME,
							awsStaticCredentialsProvider,
							awsStaticCredentialsProvider,
							awsStaticCredentialsProvider,
							workerId);
			kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);

			IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();

//			Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);
			Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, amazonKinesis(), dynamoDBClientAsync(), cloudWatch());

			System.out.printf("Running %s to process stream %s as worker %s...\n",
					SAMPLE_APPLICATION_NAME,
					SAMPLE_APPLICATION_STREAM_NAME,
					workerId);

			int exitCode = 0;
			try {
				worker.run();
			} catch (Throwable t) {
				System.err.println("Caught throwable while processing data.");
				t.printStackTrace();
				exitCode = 1;
			}
			System.exit(exitCode);


		}
	}

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
