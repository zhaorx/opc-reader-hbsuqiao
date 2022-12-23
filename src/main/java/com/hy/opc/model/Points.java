package com.hy.opc.model;


import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Configuration
@ConfigurationProperties("points")
public class Points {
    private List<Gas> list = new ArrayList<>();

    public Points() {
        super();
    }

    public Points(List<Gas> list) {
        super();
        this.list = list;
    }

    public List<Gas> getList() {
        return list;
    }

    public void setList(List<Gas> list) {
        this.list = list;
    }
}
