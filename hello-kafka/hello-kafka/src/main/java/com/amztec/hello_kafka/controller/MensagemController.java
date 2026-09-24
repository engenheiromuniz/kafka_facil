package com.amztec.hello_kafka.controller;

import com.amztec.hello_kafka.config.TopicoConfig;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/mensagens")
public class MensagemController {

    private final TopicoConfig topicoConfig;
	
	private final KafkaTemplate<String, String> kafkaTemplate;

	public MensagemController(KafkaTemplate<String, String> kafkaTemplate, TopicoConfig topicoConfig) {		
		this.kafkaTemplate = kafkaTemplate;		
		this.topicoConfig = topicoConfig;
	}
	
	@PostMapping
	public String enviar(@RequestParam String texto) {		
		kafkaTemplate.send(TopicoConfig.TOPICO, texto);
		return "Mensagem enviada: "+texto;
	}
	
	

}
