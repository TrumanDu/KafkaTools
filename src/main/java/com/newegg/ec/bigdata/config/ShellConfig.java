package com.newegg.ec.bigdata.config;

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.shell.jline.PromptProvider;

/**
 * @author：Truman.P.Du
 * @createDate: 2018年12月8日 上午10:11:38
 * @version:1.0
 * @description:
 */
@Configuration
public class ShellConfig {
    
	@Bean
	public PromptProvider promptProvider() {
		return () -> new AttributedString("KafkaTools=>", AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW));
	}
}


