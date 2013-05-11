package com.futureseeds.zookeeper;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.AbstractResource;

public class ZookeeperResource extends AbstractResource implements ApplicationContextAware, DisposableBean {
    private static final String URL_HEADER = "zk://";
    private static Log log = LogFactory.getLog(ZookeeperResource.class);
    private boolean printInputStream = false;

    public static enum ReloadContext {
        AUTO, HOLD
    };

    public static enum OnConnectionFailed {
        IGNORE, THROW_EXCEPTION
    };

    public static enum PingCmd {
        get, ls
    }

    AbstractApplicationContext ctx;

    private String znodes;
    private String connString;
    private String chKCmd;
    private boolean connectFailed = false;

    private boolean zkResouceEnable = true;
    private boolean regression;
    private OnConnectionFailed onConnectionFailed;
    private ReloadContext reloadContext;
    private RefreshContextWatcher watcher;
    private ZkExecutor executor;

    public ZookeeperResource() {
        try {
            Properties zkCfg = getZkCfg();
            this.zkResouceEnable = Boolean.parseBoolean(zkCfg.get("enable").toString());
            this.connString = zkCfg.getProperty("server");
            this.znodes = zkCfg.getProperty("znodes");
            this.chKCmd = generateZkPingCmd(PingCmd.valueOf(zkCfg.getProperty("ping_cmd")));
            this.regression = Boolean.parseBoolean(zkCfg.get("regression").toString());
            this.onConnectionFailed = OnConnectionFailed.valueOf(zkCfg.get("on_connection_failed").toString());
            this.reloadContext = ReloadContext.valueOf(zkCfg.get("reload_context").toString());
        } catch (IOException e) {
            if (onConnectionFailed == OnConnectionFailed.THROW_EXCEPTION) {
                throw new org.springframework.context.ApplicationContextException(
                        "Failed to acess /config/zk.properties", e);
            } else {
                log.error("Failed to acess /config/zk.properties", e);
            }
            connectFailed = true;
        }
    }

    public ZookeeperResource(String connString, String znodes, PingCmd chkCmd, boolean regression,
            OnConnectionFailed onConnectionFailed, ReloadContext reloadContext) {
        this.connString = connString;
        this.znodes = znodes;
        this.chKCmd = generateZkPingCmd(chkCmd);
        this.regression = regression;
        this.reloadContext = reloadContext;
        this.onConnectionFailed = onConnectionFailed;
    }

    private Properties getZkCfg() throws IOException {
        Properties props = new Properties();
        props.load(getClass().getResourceAsStream("/config/zk.properties"));
        return props;
    }

    private String generateZkPingCmd(PingCmd pingCmd) {
        return "zkCli -server " + connString + " " + pingCmd.toString() + " " + znodes.split(",")[0];
    }

    private ZkExecutor startZkClientThread() throws IOException, InterruptedException {
        log.info("Start connecting to zookeeper server: " + this.connString + ", znodes:" + znodes + " regression: "
                + regression);
        this.watcher = new RefreshContextWatcher(ctx, this.regression, this.reloadContext);
        ZkExecutor zkExecutor = new ZkExecutor(this);
        synchronized (this) {
            new Thread(zkExecutor).start();
            this.wait();
        }
        log.info("Zookeeper server connected");
        return zkExecutor;

    }

    static class StreamWriter extends Thread {
        Log log;

        BufferedReader br;

        StreamWriter(InputStream inputStream, Log log) {
            this.br = new BufferedReader(new InputStreamReader(inputStream));
            this.log = log;
            start();
        }

        public void run() {
            String line = null;
            try {
                while ((line = br.readLine()) != null) {
                    log.info(line);
                }
            } catch (IOException e) {
                log.error("failed to log ZK process.", e);
            }
        }
    }

    private static class ZkExecutor implements Runnable, Watcher, DataMonitorListener {
        private static Log log = LogFactory.getLog(ZkExecutor.class);

        private String cmd;
        private Process child;
        private ZooKeeper zk;
        private DataMonitor dm;
        private String znodes;
        private ZookeeperResource zkRes;

        private boolean zkResStarted = false;

        public ZkExecutor(ZookeeperResource zkRes) throws IOException {

            this.znodes = zkRes.znodes;
            this.cmd = zkRes.chKCmd;
            this.zk = new ZooKeeper(zkRes.connString, 3000, this);
            this.dm = new DataMonitor(zk, znodes, zkRes.watcher, this);
            this.zkRes = zkRes;
        }

        @Override
        public void process(WatchedEvent event) {
            dm.process(event);
            // at the first time ZK message send back, unlock zk resource object
            // to finish the init.
            if (!zkResStarted) {
                synchronized (zkRes) {
                    zkRes.notify();
                    zkResStarted = true;
                }
            }
        }

        @Override
        public void run() {
            try {
                synchronized (this) {
                    while (!dm.dead) {
                        wait();
                    }
                }
            } catch (InterruptedException e) {
                log.error(e);
            }

        }

        @Override
        public void exists(byte[] data) {
            if (data == null) {
                if (child != null) {
                    log.info("Stopping ZK process.");
                    child.destroy();
                    try {
                        child.waitFor();
                    } catch (InterruptedException e) {
                        log.error("Error found when waiting for ZK process stopping", e);
                    }
                }
                child = null;
            } else {
                if (child != null) {
                    log.info("Stopping ZK process.");
                    child.destroy();
                    try {
                        child.waitFor();
                    } catch (InterruptedException e) {
                        log.error("Error found when waiting for ZK process stopping", e);
                    }
                }
                log.info("Load config from zookeeper:\n" + new String(data));
                try {
                    log.info("Starting ZK process.");
                    String _cmd = cmd;
                    String osName = System.getProperty("os.name");
                    if (osName != null && osName.toUpperCase().startsWith("WIN")) {
                        _cmd = "cmd.exe " + cmd;
                    }
                    log.info("executing " + _cmd);

                    child = Runtime.getRuntime().exec(_cmd);

                    new StreamWriter(child.getInputStream(), log);
                    new StreamWriter(child.getErrorStream(), log);
                } catch (IOException e) {
                    log.error("Failed to start ZK process.", e);
                }
            }

        }

        @Override
        public void closing(int rc) {
            synchronized (this) {
                notifyAll();
            }

        }

        public ZooKeeper getZk() {
            return this.zk;
        }
    }

    public interface DataMonitorListener {
        void exists(byte[] data);

        void closing(int rc);
    }

    private static class DataMonitor implements Watcher, StatCallback {

        public boolean dead;
        private DataMonitorListener listener;
        private ZooKeeper zk;
        private Watcher chainedWatcher;
        private byte prevData[];

        public DataMonitor(ZooKeeper zk, String znodes, Watcher watcher, DataMonitorListener listener) {
            this.zk = zk;
            this.chainedWatcher = watcher;
            this.listener = listener;
            for (String znode : znodes.split(",")) {
                zk.exists(znode, true, this, null);
            }
        }

        @Override
        public void process(WatchedEvent event) {
            String path = event.getPath();
            if (event.getType() == Event.EventType.None) {
                // We are are being told that the state of the
                // connection has changed
                switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    // It's all over
                    dead = true;
                    listener.closing(KeeperException.Code.SESSIONEXPIRED.intValue());
                    break;
                default:
                    log.info("Recevied zk change with unknow status:" + event.getState() + ", skip.");
                    break;
                }
            } else {
                if (path != null && path.equals(path)) {
                    // Something has changed on the node, let's find out
                    zk.exists(path, true, this, null);
                }
            }
            if (chainedWatcher != null) {
                chainedWatcher.process(event);
            }
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            boolean exists;
            Code code = Code.get(rc);
            switch (code) {
            case OK:
                exists = true;
                break;
            case NONODE:
                exists = false;
                break;
            case SESSIONEXPIRED:
            case NOAUTH:
                dead = true;
                listener.closing(rc);
                return;
            default:
                // Retry errors
                zk.exists(path, true, this, null);
                return;
            }

            byte b[] = null;
            if (exists) {
                try {
                    b = zk.getData(path, false, null);
                } catch (KeeperException e) {
                    // We don't need to worry about recovering now. The watch
                    // callbacks will kick off any exception handling
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    return;
                }
            }
            if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
                listener.exists(b);
                prevData = b;
            }
        }

    }

    public ZooKeeper getZk() {
        return executor.getZk();
    }

    @Override
    public boolean exists() {
        try {
            Stat stat = getZk().exists(znodes, false);
            return null != stat;
        } catch (Exception e) {
            log.error("Falied to detect the config in zoo keeper.", e);
            return false;
        }
    }

    @Override
    public boolean isOpen() {
        return false;
    }

    @Override
    public URL getURL() throws IOException {
        return new URL(URL_HEADER + connString + znodes);
    }

    @Override
    public String getFilename() throws IllegalStateException {
        return znodes;
    }

    @Override
    public String getDescription() {
        return "Zookeeper resouce at '" + URL_HEADER + connString + ", zonode: '" + znodes + "'. Enabled: "
                + zkResouceEnable;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (executor == null || !executor.zkResStarted) {
            try {
                this.executor = startZkClientThread();
            } catch (Exception e) {
                if (onConnectionFailed == OnConnectionFailed.THROW_EXCEPTION) {
                    throw new org.springframework.context.ApplicationContextException("Failed to connect to zk server"
                            + this.connString, e);
                } else {
                    log.error("Failed to connect to zk server:" + this.connString, e);
                }
                connectFailed = true;
            }
        }

        if (!zkResouceEnable) {
            // disabled tools, return nothing;
            log.info("Zookeeper resource disbaled, skip loading resource from Zookeeper server.");
            return new ByteArrayInputStream(new byte[0]);
        }

        if (connectFailed) {
            // init failed, but set to continue, return nothing;
            return new ByteArrayInputStream(new byte[0]);
        } else {
            try {
                SequenceInputStream pis = generateSequenceInputStream(znodes, regression);
                if (printInputStream) {
                    String ouput = getString(pis);
                    log.info(ouput);
                    return new ByteArrayInputStream(ouput.getBytes());
                } else {
                    return pis;
                }
            } catch (Exception e) {
                throw new IOException("Fail to get inputstream from zookeeper", e);
            }
        }
    }

    private String getString(SequenceInputStream pis) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(pis));
        String line = null;
        StringBuilder sb = new StringBuilder();
        sb.append("\n");
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    private SequenceInputStream generateSequenceInputStream(String znodes, boolean regressionZnodes)
            throws KeeperException, InterruptedException {
        List<InputStream> seqenceInputStreamCollector = new ArrayList<InputStream>();
        for (String znode : znodes.split(",")) {
            getDataInputStream(znode, seqenceInputStreamCollector, regressionZnodes);
        }
        return new SequenceInputStream(Collections.enumeration(seqenceInputStreamCollector));
    }

    private void getDataInputStream(String currentZnode, List<InputStream> seqIsCollector, boolean regressionZnodes)
            throws KeeperException, InterruptedException {
        ZooKeeper zk = getZk();
        seqIsCollector.add(new ByteArrayInputStream(zk.getData(currentZnode, true, null)));
        // need add return between every stream otherwise top/last line will be
        // join to one line.
        seqIsCollector.add(new ByteArrayInputStream("\n".getBytes()));
        if (regressionZnodes) {
            List<String> children = zk.getChildren(currentZnode, true);
            if (children != null) {
                for (String child : children) {
                    String childZnode = currentZnode + "/" + child;
                    getDataInputStream(childZnode, seqIsCollector, regressionZnodes);
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = (AbstractApplicationContext) ctx;
    }

    @Override
    public void destroy() throws Exception {
        log.info("Destory Zookeeper Resouce.");
        if (executor != null) {
            log.info("Close connection to Zookeeper Server.");
            try {
                executor.getZk().close();
                log.info("Connection to Zookeeper Server closed.");
            } catch (Exception e) {
                log.error("Error found when close zookeeper connection.", e);
            }
        }

    }

}
