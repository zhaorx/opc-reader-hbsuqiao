package com.hy.opc;


import com.hy.opc.model.Gas;
import com.hy.opc.model.Points;
import com.hy.opc.model.WritterResult;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class SchedulerTask {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerTask.class);
    @Value("${push-url}")
    private String pushUrl;
    @Value("${push-multi-url}")
    private String pushMultiUrl;
    @Value("${opc-url}")
    private String opcUrl;
    @Value("${region}")
    private String region;
    @Autowired
    private Points points;

    private String sep = "_";
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Resource
    private OpcConnector connector;

    private OpcUaClient opcUaClient;

    @Scheduled(fixedDelayString = "${interval}")
    public void transferSchedule() throws Exception {
        logger.info("starting transfer...");

//        this.printNodeTree();

        List<Gas> addList = new ArrayList<>();
        List<Gas> list = points.getList();
        String dateStr = sdf.format(new Date());
        for (int i = 0; i < list.size(); i++) {
            Gas item = list.get(i);
            Gas g = new Gas();
            g.setTs(dateStr);
            g.setPoint(region + sep + item.getTableName());
            g.setPname(region + sep + item.getPname());
            g.setUnit(item.getUnit());
            g.setRegion(region);
            Double value = this.getPointValue(item.getPoint());
            if (value == Double.MIN_VALUE) {
                continue;
            }
            g.setValue(value);
            logger.debug("######point_data:" + g.toString());
            addList.add(g);
        }

        WritterResult result = this.addMultiTaos(addList);
        logger.info(result.getMessage());
    }

    public void printNodeTree() throws Exception {
        //创建OPC UA客户端
        if (opcUaClient == null) {
            opcUaClient = connector.createClient(opcUrl);
            //开启连接
            opcUaClient.connect().get();
        }

        connector.browseNode(opcUaClient, null);
    }

    public Double getPointValue(String point) throws Exception {
        int point_int = Integer.parseInt(point);
        //创建OPC UA客户端
        if (opcUaClient == null) {
            opcUaClient = connector.createClient(opcUrl);
            //开启连接
            opcUaClient.connect().get();
        }

        //读
        DataValue data = connector.readNodeByInt(opcUaClient, point_int);
        if (data.getValue().getValue() == null) {
            return Double.MIN_VALUE;
        }

        String s = String.valueOf(data.getValue().getValue());
        return Double.parseDouble(s);
    }

    public WritterResult addMultiTaos(List<Gas> list) {
        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON);
        RestTemplate restTemplate = new RestTemplate();
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("gasList", list);

        HttpEntity<Map<String, Object>> r = new HttpEntity<>(requestBody, requestHeaders);
        WritterResult result = restTemplate.postForObject(pushMultiUrl, r, WritterResult.class);

        return result;
    }

    private WritterResult addTaos(Gas data) {
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
        WritterResult result = restTemplate.postForObject(url, r, WritterResult.class);

        return result;

    }

    private WritterResult batchAddTaos(List<Gas> list) {
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
        WritterResult result = restTemplate.postForObject(pushUrl, r, WritterResult.class);
        return result;
    }
}
