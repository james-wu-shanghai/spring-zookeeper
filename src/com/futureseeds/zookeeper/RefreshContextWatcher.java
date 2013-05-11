package com.futureseeds.zookeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.springframework.context.support.AbstractApplicationContext;

import com.futureseeds.zookeeper.ZookeeperResource.ReloadContext;

public class RefreshContextWatcher implements Watcher {

    private static Log log = LogFactory.getLog(RefreshContextWatcher.class);

    private AbstractApplicationContext ctx;
    private boolean regressionZnodes;
    private ReloadContext reloadContext;

    public RefreshContextWatcher(AbstractApplicationContext ctx, boolean regressionZnodes, ReloadContext reloadContext) {
        this.ctx = ctx;
        this.reloadContext = reloadContext;
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
        case NodeChildrenChanged:
            if (!regressionZnodes) {
                break;
            }
        case NodeDataChanged:
            log.info("Detected ZNode or sub ZNode changed.");
            switch (reloadContext) {
            case AUTO:
                log.info("Refresh spring context.");
                ctx.refresh();
                break;
            case HOLD:
                log.info("Keep context unchange according to configuration.");
                break;
            }
            break;
        case NodeDeleted:
            log.warn("Warnning! ZK Node for application config has been removed!");
            break;
        default:
            log.info("Zk Node changed, type" + event.getType() + " Stat:" + event.getState() + ".");
            break;
        }
    }
}
