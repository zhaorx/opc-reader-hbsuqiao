package com.hy;

import com.hy.opc.OPCReaderApplication;
import com.hy.opc.OpcConnector;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;

@SpringBootTest(classes = OPCReaderApplication.class)
@RunWith(SpringRunner.class)
class OPCReaderApplicationTests {

    @Resource
    public OpcConnector opcConnector;

    @Value("${opc-url}")
    private String opcUrl;

    @Test
    public void testOPCClient() throws Exception {
        //创建OPC UA客户端
        OpcUaClient opcUaClient = opcConnector.createClient(opcUrl);

        //开启连接
        opcUaClient.connect().get();

        //遍历节点
        opcConnector.browseNode(opcUaClient, null);

        //读
        opcConnector.readNodeByString(opcUaClient, "模拟器示例.函数.Ramp1");


        //订阅
        opcConnector.subscribe(opcUaClient, "模拟器示例.函数.Ramp1");

        //批量订阅
//        managedSubscriptionEvent(opcUaClient);

        //关闭连接
        opcUaClient.disconnect().get();
    }
}
