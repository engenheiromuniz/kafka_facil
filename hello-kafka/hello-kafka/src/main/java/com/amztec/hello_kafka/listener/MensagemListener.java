package com.amztec.hello_kafka.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.amztec.hello_kafka.config.TopicoConfig;

@Component
public class MensagemListener {
	
	private static final Logger log = LoggerFactory.getLogger(MensagemListener.class);
	@KafkaListener(topics = TopicoConfig.TOPICO, groupId = "hello-group")
	public void ouvir(ConsumerRecord<String, String> registro) {
		log.info("Recebido: '{}' | partição={} | offse={}",
				registro.value(), registro.partition(), registro.offset());
	}

}
