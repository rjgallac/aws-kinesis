package com.example.demo;

import com.example.demo.config.KinesisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

@SpringBootApplication
public class DemoApplication {

	@Autowired
	private KinesisService kinesisService;

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

	@Component
	public class ApplicationRunnerBean implements ApplicationRunner {
		@Override
		public void run(ApplicationArguments arg0) throws Exception {
			kinesisService.startKinesis();
		}
	}

}
