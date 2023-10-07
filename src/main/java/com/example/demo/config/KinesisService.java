package com.example.demo.config;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

@Service
public class KinesisService {

    private static final String SAMPLE_APPLICATION_NAME = "test-app";
    public static final String SAMPLE_APPLICATION_STREAM_NAME = "test-stream";

    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;


    @Autowired
    private AmazonKinesis amazonKinesis;

    @Autowired
    private AmazonDynamoDB amazonDynamoDB;

    @Autowired
    private AmazonCloudWatch amazonCloudWatch;

    @Value("${CONSUMER_NAME}")
    private String consumerName;

    public void startKinesis() throws UnknownHostException {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("dummy", "dummy");
        AWSCredentialsProvider awsStaticCredentialsProvider = new AWSStaticCredentialsProvider(awsCredentials);
        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID() + ":" + consumerName;

        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        awsStaticCredentialsProvider,
                        awsStaticCredentialsProvider,
                        awsStaticCredentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);

        IRecordProcessorFactory recordProcessorFactory = new AmazonKinesisApplicationRecordProcessorFactory();

        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, amazonKinesis, amazonDynamoDB, amazonCloudWatch);

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
