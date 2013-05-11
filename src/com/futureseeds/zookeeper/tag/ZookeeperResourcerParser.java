package com.futureseeds.zookeeper.tag;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

import com.futureseeds.zookeeper.ZookeeperResource;
import com.futureseeds.zookeeper.ZookeeperResource.OnConnectionFailed;
import com.futureseeds.zookeeper.ZookeeperResource.PingCmd;
import com.futureseeds.zookeeper.ZookeeperResource.ReloadContext;

public class ZookeeperResourcerParser extends AbstractSimpleBeanDefinitionParser {

    private static enum InitializeBy {
        CONSTRUCTOR_ARGS, LOCAL_FILE
    };

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        InitializeBy initializeBy = InitializeBy.valueOf(element.getAttribute("initializeBy"));
        switch (initializeBy) {
        case CONSTRUCTOR_ARGS:
            builder.addConstructorArgValue(element.getAttribute("server"))
                    .addConstructorArgValue(element.getAttribute("znodes"))
                    .addConstructorArgValue(PingCmd.valueOf(element.getAttribute("pingCmd")))
                    .addConstructorArgValue(Boolean.valueOf(element.getAttribute("regression")))
                    .addConstructorArgValue(OnConnectionFailed.valueOf(element.getAttribute("onConnectionFailed")))
                    .addConstructorArgValue(ReloadContext.valueOf(element.getAttribute("reloadContext")));
            break;
        case LOCAL_FILE:
            break;
        }
    }

    @Override
    protected Class<ZookeeperResource> getBeanClass(Element element) {
        return ZookeeperResource.class;
    }

}
