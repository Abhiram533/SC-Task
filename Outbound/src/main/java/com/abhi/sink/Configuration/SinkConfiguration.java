package com.abhi.sink.Configuration;

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
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.abhi.sink.model.Usage;
@Configuration
@EnableTask
@EnableBatchProcessing
public class SinkConfiguration {
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Value("${usage.file.name:classpath:usageinfo.json}")
	private Resource usageResource;

	@Bean
	public Job job1(ItemReader<Usage> reader, ItemProcessor<Usage, Usage> itemProcessor, ItemWriter<Usage> writer) {
		Step step = stepBuilderFactory.get("SINK")
				.<Usage, Usage>chunk(1)
				.reader(reader)
				.processor(itemProcessor)
				.writer(writer)
				.build();

		return jobBuilderFactory.get("SINK")
				.incrementer(new RunIdIncrementer())
				.start(step)
				.build();
	}

	
	  @Bean public ItemReader<Usage> ItemReader(DataSource dataSource){ return new
	  JdbcCursorItemReaderBuilder<Usage>() .name("Abhiram") .dataSource(dataSource)
	  .sql("SELECT * FROM TAKARA_FINAL") .rowMapper(new
	  BeanPropertyRowMapper<>(Usage.class)) .build(); }
	 
	  @Bean
	    public FlatFileItemWriter<Usage> writer() 
	    {
	        //Create writer instance
	        FlatFileItemWriter<Usage> writer = new FlatFileItemWriter<>();
	         
	        //Set output file location
	        writer.setResource(new FileSystemResource("C:\\Users\\Administrator\\OneDrive\\Desktop\\Takara\\output.csv"));
	         
	        //All job repetitions should "append" to same output file
	        writer.setAppendAllowed(true);
	         
	        //Name field values sequence based on object properties 
	        writer.setLineAggregator(new DelimitedLineAggregator<Usage>() {
	            {
	                setDelimiter(",");
	                setFieldExtractor(new BeanWrapperFieldExtractor<Usage>() {
	                    {
	                        setNames(new String[] { "id", "firstName", "lastName","minutes","dataUsage" });
	                    }
	                });
	            }
	        });
	        return writer;
	    }
	  
	 
	/*
	 * @Bean public ItemWriter<Usage> jdbcBillWriter(DataSource dataSource) {
	 * JdbcBatchItemWriter<Usage> writer = new JdbcBatchItemWriterBuilder<Usage>()
	 * .beanMapped() .dataSource(dataSource)
	 * .sql("INSERT INTO TAKARA (id, first_name, last_name, minutes, data_usage) VALUES (:id, :firstName, :lastName, :minutes, :dataUsage)"
	 * ) .build(); return writer; }
	 */

	@Bean
	ItemProcessor<Usage, Usage> billProcessor() {
		return new Processor();
	}
}
