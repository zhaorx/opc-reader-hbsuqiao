package com.hy.opc;


import com.hy.opc.model.Gas;
import com.hy.opc.model.Result;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class SchedulerTask {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerTask.class);
    @Value("${push-url}")
    private String pushUrl;
    @Value("${opc-url}")
    private String opcUrl;

    @Resource
    private OpcConnector connector;

    private OpcUaClient opcUaClient;

    private String point = "模拟器示例.函数.Ramp8";

    @Scheduled(fixedDelayString = "${interval}")
    public void transferSchedule() throws Exception {
        logger.info("starting transfer...");
        Gas gas = this.getRecentGas();
        Result result = this.addTaos(gas);
        logger.info(result.getMessage());
//        Result result = this.addTaos(list.get(0));
//        System.out.println(result.getMessage());
    }

    private Result addTaos(Gas data) {
        String url = "http://localhost:6666/gas/add";
        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);
        RestTemplate restTemplate = new RestTemplate();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = sdf.format(data.getTs());

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("ts", dateString);
        requestBody.put("point", data.getPoint());
        requestBody.put("pname", data.getPname());
        requestBody.put("unit", data.getUnit());
        requestBody.put("region", data.getRegion());
        requestBody.put("value", data.getValue());

        HttpEntity<Map<String, Object>> r = new HttpEntity<>(requestBody, requestHeaders);

        // 请求服务端添加玩家
        Result result = restTemplate.postForObject(url, r, Result.class);

        return result;

    }

    private Result batchAddTaos(List<Gas> list) {
        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);
        RestTemplate restTemplate = new RestTemplate();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        List<String> tsList = list.stream().map(item -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(item.getTs())).collect(Collectors.toList());
        List<Double> valueList = list.stream().map(Gas::getValue).collect(Collectors.toList());

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("tss", tsList);
        requestBody.put("values", valueList);
        requestBody.put("point", list.get(0).getPoint());
        requestBody.put("pname", list.get(0).getPname());
        requestBody.put("unit", list.get(0).getUnit());
        requestBody.put("region", list.get(0).getRegion());
        HttpEntity<Map<String, Object>> r = new HttpEntity<>(requestBody, requestHeaders);

        // 请求服务端添加玩家
        Result result = restTemplate.postForObject(pushUrl, r, Result.class);
        return result;
    }


    public Gas getRecentGas() throws Exception {
        //创建OPC UA客户端
        if (opcUaClient == null) {
            opcUaClient = connector.createClient(opcUrl);
            //开启连接
            opcUaClient.connect().get();
        }

        //读
        DataValue data = connector.readNode(opcUaClient, point);

        Gas gas = new Gas();
//        gas.setTs(new Date(data.getSourceTime().getJavaTime()));
        gas.setTs(new Date());
        gas.setValue((Double) data.getValue().getValue());
        gas.setPoint("testOPC");
        gas.setPname("testOPC");
        gas.setUnit("m3/s");
        gas.setRegion("xinjiang");

        return gas;
    }
}
