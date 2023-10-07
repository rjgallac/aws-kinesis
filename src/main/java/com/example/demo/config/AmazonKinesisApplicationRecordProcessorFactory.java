package com.example.demo.config;


import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class AmazonKinesisApplicationRecordProcessorFactory implements IRecordProcessorFactory {
    /**
     * {@inheritDoc}
     */
    @Override
    public IRecordProcessor createProcessor() {
        return new AmazonKinesisApplicationSampleRecordProcessor();
    }
}