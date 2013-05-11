package com.futureseeds.zookeeper.config;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.futureseeds.zookeeper.ZookeeperConfigurer;
import com.futureseeds.zookeeper.ZookeeperResource;
import com.futureseeds.zookeeper.ZookeeperResource.OnConnectionFailed;
import com.futureseeds.zookeeper.ZookeeperResource.PingCmd;
import com.futureseeds.zookeeper.ZookeeperResource.ReloadContext;

public class ZookeeperResourceTest {
    private static Log log = LogFactory.getLog(ZookeeperResourceTest.class);
    final int CONTEXT_SWITCH_GAP = 15000;

    ApplicationContext ctx;

    private String testData;

    public String getTestData() {
        return testData;
    }

    public void setTestData(String testData) {
        this.testData = testData;
    }

    @Before
    public void before() throws Exception {
        ctx = new ClassPathXmlApplicationContext("/common-unit.xml");
    }

    @Test
    public void getZkConfigurer() {
        ZookeeperConfigurer zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        assertEquals("jdbc:mysql://172.16.100.34:3306/testdb?useUnicode=true&amp;characterEncoding=utf-8",
                zkConf.getProperty("mysql.testdb.jdbc.url"));
    }

    @Test
    public void startClient() throws InterruptedException, IOException, KeeperException {
        ZookeeperResource testee = new ZookeeperResource("127.0.0.1:2181", "/zk_test", PingCmd.get,
                false, OnConnectionFailed.THROW_EXCEPTION, ReloadContext.AUTO);
        BufferedReader br = new BufferedReader(new InputStreamReader(testee.getInputStream()));
        String line = null;
        while ((line = br.readLine()) != null) {
            log.info(line);
        }
    }

    @Ignore
    @Test
    public void insertData2Zk() throws KeeperException, InterruptedException, IOException {
        ZookeeperConfigurer zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        ZookeeperResource zkResource = (ZookeeperResource) zkConf.getZkResoucre();
        ZooKeeper zk = zkResource.getZk();
        try {
            zk.delete("/cn_dev", -1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String fileName = "/zk_cnfig_cn_test.txt";
        String fileContent = getFileContent(fileName);
        zk.create("/cn_dev", fileContent.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private String getFileContent(String fileName) throws FileNotFoundException, IOException {
        InputStream is = this.getClass().getResourceAsStream(fileName);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line = null;
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public void setData2ZkBack() throws KeeperException, InterruptedException {
        ZookeeperResource zkResource = getZkResource();
        String data = "#data\tsource\nmysql.testdb.jdbc.type=testdb\nmysql.testdb.jdbc.driverClassName=com.mysql.jdbc.Driver\nmysql.testdb.jdbc.url=jdbc:mysql://172.16.100.34:3306/testdb?useUnicode=true&amp;characterEncoding=utf-8\nmysql.testdb.jdbc.username=subsadmin\nmysql.testdb.jdbc.password=rdyh45td\n\noracle.pna.jdbc.type=oracle\noracle.pna.jdbc.driverClassName=oracle.jdbc.OracleDriver\noracle.pna.jdbc.url=jdbc:oracle:thin:@172.16.100.40:1521:pna\noracle.pna.jdbc.username=pnaadmin\noracle.pna.jdbc.password=rdyh45td\noracle.pna.pool.maxActive=5\noracle.pna.pool.maxIdle=1\noracle.pna.pool.maxWait=30000\n\nmysql.provision.name=provision\nmysql.provision.jdbc.driverClassName=com.mysql.jdbc.Driver\nmysql.provision.jdbc.url=jdbc:mysql://172.16.100.34:3318/dapdb?useUnicode=true&amp;characterEncoding=utf-8\nmysql.provision.jdbc.username=dapadmin\nmysql.provision.jdbc.password=rdyh45td\n\nmysql.datafix.name=datafix\nmysql.datafix.jdbc.driverClassName=com.mysql.jdbc.Driver\nmysql.datafix.jdbc.url=jdbc:mysql://172.16.100.34:3318/dapdb?useUnicode=true&amp;characterEncoding=utf-8\nmysql.datafix.jdbc.username=dapadmin\nmysql.datafix.jdbc.password=rdyh45td\n\nmysql.quartz.name=quartz\nmysql.quartz.jdbc.driverClassName=com.mysql.jdbc.Driver\nmysql.quartz.jdbc.url=jdbc:mysql://172.16.100.34:3318/dapschedulerdb?useUnicode=true&amp;characterEncoding=utf-8\nmysql.quartz.jdbc.username=admin\nmysql.quartz.jdbc.password=admin";
        zkResource.getZk().setData("/cn_test", data.getBytes(), -1);
    }

    @Test
    public void triggerRefreshWatcher() throws KeeperException, InterruptedException, FileNotFoundException,
            IOException {
        ZookeeperConfigurer zkConf = null;
        setData2ZkDev();

        Thread.sleep(CONTEXT_SWITCH_GAP);
        zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        assertEquals("jdbc:mysql://172.16.100.36:3306/testdb?useUnicode=true&amp;characterEncoding=utf-8",
                zkConf.getProperty("mysql.testdb.jdbc.url"));
        assertEquals("6", zkConf.getProperty("data"));
        setData2ZkDevBack();
        Thread.sleep(CONTEXT_SWITCH_GAP);
        zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        assertEquals("jdbc:mysql://172.16.100.34:3306/testdb?useUnicode=true&amp;characterEncoding=utf-8",
                zkConf.getProperty("mysql.testdb.jdbc.url"));

    }

    private void setData2ZkDevBack() throws FileNotFoundException, IOException, KeeperException, InterruptedException {
        log.info("set data to zk dev back.");
        ZookeeperResource zkResource = getZkResource();
        ZooKeeper zk = zkResource.getZk();
        String fileName = "/zk_cnfig_cn_test.txt";
        String fileContent = getFileContent(fileName);
        zk.setData("/cn_dev", fileContent.getBytes(), -1);
    }

    private ZookeeperResource getZkResource() {
        ZookeeperConfigurer zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        ZookeeperResource zkResource = zkConf.getZkResoucre();
        return zkResource;
    }

    private void setData2ZkDev() throws FileNotFoundException, IOException, KeeperException, InterruptedException {
        log.info("set data to zk dev.");
        ZookeeperResource zkResource = getZkResource();
        ZooKeeper zk = zkResource.getZk();
        String fileName = "/zk_cnfig_cn_test_2.txt";
        String fileContent = getFileContent(fileName);
        zk.setData("/cn_dev", fileContent.getBytes(), -1);

    }

    @Test
    public void testSub() throws KeeperException, InterruptedException {
        Boolean isTestSub = getTestSub();
        reverseTestSub(isTestSub);
        Thread.sleep(CONTEXT_SWITCH_GAP);
        Boolean isTestSub2 = getTestSub();
        assertEquals(isTestSub, !isTestSub2);
    }

    private void reverseTestSub(Boolean isTestSub) throws KeeperException, InterruptedException {
        ZookeeperResource zkResource = getZkResource();
        ZooKeeper zk = zkResource.getZk();
        zk.setData("/cn_dev/test_sub", ("testSub=" + !isTestSub).toString().getBytes(), -1);
    }

    private Boolean getTestSub() {
        ZookeeperConfigurer zkConf = (ZookeeperConfigurer) ctx.getBean("zkPropConfigurer");
        Boolean isTestSub = Boolean.valueOf(zkConf.getProperty("testSub").toString());
        assertNotNull(isTestSub);
        return isTestSub;
    }
}
