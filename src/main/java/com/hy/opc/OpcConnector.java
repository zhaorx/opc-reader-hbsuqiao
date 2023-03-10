package com.hy.opc;

import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.api.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.api.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscription;
import org.eclipse.milo.opcua.sdk.client.api.subscriptions.UaSubscriptionManager;
import org.eclipse.milo.opcua.sdk.client.nodes.UaNode;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedDataItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.ManagedSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.SecurityPolicy;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoredItemCreateRequest;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringParameters;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class OpcConnector {
    public Logger logger = LoggerFactory.getLogger(OpcConnector.class);
    //    public final String endPointUrl = "opc.tcp://localhost:49320";
    public AtomicInteger atomic = new AtomicInteger(1);

    @Value("${username}")
    private String username;
    @Value("${password}")
    private String password;


    /**
     * ??????OPC UA?????????
     *
     * @return
     * @throws Exception
     */
    public OpcUaClient createClient(String opcUrl) throws Exception {
        //opc ua???????????????
        Path securityTempDir = Paths.get(System.getProperty("java.io.tmpdir"), "security");
        Files.createDirectories(securityTempDir);
        if (!Files.exists(securityTempDir)) {
            throw new Exception("unable to create security dir: " + securityTempDir);
        }
        return OpcUaClient.create(opcUrl,
                endpoints ->
                        endpoints.stream()
                                .filter(e -> e.getSecurityPolicyUri().equals(SecurityPolicy.None.getUri()))
                                .findFirst(),
                configBuilder ->
                        configBuilder
                                .setApplicationName(LocalizedText.english("eclipse milo opc-ua client"))
                                .setApplicationUri("urn:eclipse:milo:examples:client")
                                //????????????
                                .setIdentityProvider(new UsernameProvider(username, password))
//                                .setIdentityProvider(new AnonymousProvider())
                                .setRequestTimeout(UInteger.valueOf(5000))
                                .build()
        );
    }

    /**
     * ??????????????????
     *
     * @param client OPC UA?????????
     * @throws Exception
     */
    public DataValue readNodeByString(OpcUaClient client, String point) throws Exception {
        int namespaceIndex = 2;
        //??????
        NodeId nodeId = new NodeId(namespaceIndex, point);

        //??????????????????
        DataValue value = client.readValue(0.0, TimestampsToReturn.Server, nodeId).get();
        logger.debug("######read_node:" + String.valueOf(nodeId.getIdentifier()) + ": " + String.valueOf(value.getValue().getValue()));
        return value;
    }

    /**
     * ??????????????????
     *
     * @param client OPC UA?????????
     * @throws Exception
     */
    public DataValue readNodeByInt(OpcUaClient client, int point) throws Exception {
        int namespaceIndex = 2;
        //??????
        NodeId nodeId = new NodeId(namespaceIndex, point);

        //??????????????????
        DataValue value = client.readValue(0.0, TimestampsToReturn.Server, nodeId).get();
        logger.debug("######read_node:" + String.valueOf(nodeId.getIdentifier()) + ": " + String.valueOf(value.getValue().getValue()));
        return value;
    }

    /**
     * ??????(??????)
     *
     * @param client
     * @throws Exception
     */
    public void subscribe(OpcUaClient client, String point) throws Exception {
        //??????????????????1000ms???????????????
        client
                .getSubscriptionManager()
                .createSubscription(1000.0)
                .thenAccept(t -> {
                    //??????
                    NodeId nodeId = new NodeId(2, point);
                    ReadValueId readValueId = new ReadValueId(nodeId, AttributeId.Value.uid(), null, null);
                    //?????????????????????
                    MonitoringParameters parameters = new MonitoringParameters(UInteger.valueOf(atomic.getAndIncrement()), 1000.0, null, UInteger.valueOf(10), true);
                    //?????????????????????
                    //????????????????????????????????????
                    MonitoredItemCreateRequest request = new MonitoredItemCreateRequest(readValueId, MonitoringMode.Reporting, parameters);
                    List<MonitoredItemCreateRequest> requests = new ArrayList<>();
                    requests.add(request);
                    //?????????????????????????????????????????????????????????????????????
                    t.createMonitoredItems(
                            TimestampsToReturn.Both,
                            requests,
                            (item, id) -> item.setValueConsumer((it, val) -> {
                                System.out.println("nodeid :" + it.getReadValueId().getNodeId());
                                System.out.println("value :" + val.getValue().getValue());
                                // onSubscribeEvent

//                                Gas gas = new Gas();
//                                gas.setTs(new Date());
//                                gas.setValue(((Integer) val.getValue().getValue()).doubleValue());
//                                gas.setPoint(point);
//                                gas.setPname(point);
//                                gas.setUnit("m3/s");
//                                gas.setRegion("xinjiang");
//                                addTaos(gas);
                            })
                    );
                }).get();

        //????????????
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * ????????????
     *
     * @param client
     * @throws Exception
     */
    public void managedSubscriptionEvent(OpcUaClient client) throws Exception {
        final CountDownLatch eventLatch = new CountDownLatch(1);

        //??????????????????????????????????????????????????????????????????
        client.getSubscriptionManager().addSubscriptionListener(new CustomSubscriptionListener(client));

        //??????????????????
        handlerNode(client);

        //????????????
        eventLatch.await();
    }

    /**
     * ??????????????????
     *
     * @param client OPC UA?????????
     */
    public void handlerNode(OpcUaClient client) {
        try {
            //????????????
            ManagedSubscription subscription = ManagedSubscription.create(client);

            //?????????????????????key
            List<String> key = new ArrayList<>();
            key.add("TD-01.SB-01.AG-01");
            key.add("TD-01.SB-01.AG-02");

            List<NodeId> nodeIdList = new ArrayList<>();
            for (String s : key) {
                nodeIdList.add(new NodeId(2, s));
            }

            //??????
            List<ManagedDataItem> dataItemList = subscription.createDataItems(nodeIdList);
            for (ManagedDataItem managedDataItem : dataItemList) {
                managedDataItem.addDataValueListener((t) -> {
                    System.out.println(managedDataItem.getNodeId().getIdentifier().toString() + ":" + t.getValue().getValue().toString());
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ?????????????????????
     */
    public class CustomSubscriptionListener implements UaSubscriptionManager.SubscriptionListener {

        public OpcUaClient client;

        CustomSubscriptionListener(OpcUaClient client) {
            this.client = client;
        }

        public void onKeepAlive(UaSubscription subscription, DateTime publishTime) {
            logger.debug("onKeepAlive");
        }

        public void onStatusChanged(UaSubscription subscription, StatusCode status) {
            logger.debug("onStatusChanged");
        }

        public void onPublishFailure(UaException exception) {
            logger.debug("onPublishFailure");
        }

        public void onNotificationDataLost(UaSubscription subscription) {
            logger.debug("onNotificationDataLost");
        }

        /**
         * ????????? ???????????????????????????????????? ??????????????????
         *
         * @param uaSubscription ??????
         * @param statusCode     ??????
         */
        public void onSubscriptionTransferFailed(UaSubscription uaSubscription, StatusCode statusCode) {
            logger.debug("?????????????????? ??????????????????");
            //??????????????????????????????
            handlerNode(client);
        }
    }

    /**
     * ??????????????????
     *
     * @param client OPC UA?????????
     * @param uaNode ??????
     * @throws Exception
     */
    public void browseNode(OpcUaClient client, UaNode uaNode) throws Exception {
        List<? extends UaNode> nodes;
        if (uaNode == null) {
            nodes = client.getAddressSpace().browseNodes(Identifiers.ObjectsFolder);
        } else {
            nodes = client.getAddressSpace().browseNodes(uaNode);
        }
        for (UaNode nd : nodes) {
            if (Objects.requireNonNull(nd.getBrowseName().getName()).contains("_")) {
                continue;
            }
            System.out.println("Node= " + nd.getNodeId().toString() + ":" + nd.getBrowseName().getName());
            browseNode(client, nd);
        }
    }
}
