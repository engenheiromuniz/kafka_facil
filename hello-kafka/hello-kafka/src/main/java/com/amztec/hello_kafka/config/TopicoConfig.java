package com.amztec.hello_kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicoConfig {
	
	public static final String TOPICO = "hello-topic";
	
	@Bean
	NewTopic helloTopic() {
		return TopicBuilder.name(TOPICO).partitions(3).replicas(1).build();
	}
	

}
