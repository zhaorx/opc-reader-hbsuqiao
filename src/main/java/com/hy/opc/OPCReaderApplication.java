package com.hy.opc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling // 开启定时
public class OPCReaderApplication {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(OPCReaderApplication.class, args);

    }
}
