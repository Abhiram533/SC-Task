package com.abhi.source.Configuration;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import com.abhi.source.model.Usage;
import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableTask
@EnableBatchProcessing
public class SourceConfiguration {
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	//@Value("${usage.file.name:classpath:usageinfo.json}")\
	//private Resource usageResource;
	@Value("${input}")
	private String path;

	@Bean
	public Job job1(ItemReader<Usage> reader, ItemProcessor<Usage, Usage> itemProcessor, ItemWriter<Usage> writer) {
		Step step = stepBuilderFactory.get("source").<Usage, Usage>chunk(1).reader(reader).processor(itemProcessor)
				.writer(writer).build();

		return jobBuilderFactory.get("source").incrementer(new RunIdIncrementer()).start(step).build();
	}

	@Bean
	public JsonItemReader<Usage> jsonItemReader() {

		ObjectMapper objectMapper = new ObjectMapper();
		JacksonJsonObjectReader<Usage> jsonObjectReader = new JacksonJsonObjectReader<>(Usage.class);
		jsonObjectReader.setMapper(objectMapper);

		return new JsonItemReaderBuilder<Usage>().jsonObjectReader(jsonObjectReader).resource(new FileSystemResource(path))
				.name("UsageJsonItemReader").build();
	}

	@Bean
	public ItemWriter<Usage> jdbcBillWriter(DataSource dataSource) {
		JdbcBatchItemWriter<Usage> writer = new JdbcBatchItemWriterBuilder<Usage>().beanMapped().dataSource(dataSource)
				.sql("INSERT INTO TAKARA_STAGE (id, first_name, last_name, minutes, data_usage) VALUES (:id, :firstName, :lastName, :minutes, :dataUsage)")
				.build();
		return writer;
	}

	@Bean
	ItemProcessor<Usage, Usage> billProcessor() {
		return new Processor();
	}
}
