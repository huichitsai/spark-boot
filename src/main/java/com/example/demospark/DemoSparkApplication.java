package com.example.demospark;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class DemoSparkApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(DemoSparkApplication.class, args);
	}

	@Override
	public void run(String... args) {
		String logFile = "README.md"; // Should be some file on your system
		SparkSession spark = SparkSession.builder()
				.appName("Simple Application")
				.config("spark.master", "local")
				.getOrCreate();
		Dataset<String> logData = spark.read().textFile(logFile).cache();

		long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
		long numBs = logData.filter((FilterFunction<String>) s -> s.contains("b")).count();

		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

		spark.stop();
	}

}
