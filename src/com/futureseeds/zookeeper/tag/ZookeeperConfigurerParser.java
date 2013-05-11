package com.futureseeds.zookeeper.tag;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSimpleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

import com.futureseeds.zookeeper.ZookeeperConfigurer;

public class ZookeeperConfigurerParser extends AbstractSimpleBeanDefinitionParser {

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        String order = element.getAttribute("order");
        if (!StringUtils.hasText(order)) {
            order = Integer.MAX_VALUE + "";
        }
        String localLocations = element.getAttribute("localLocations");

        if (StringUtils.hasLength(localLocations)) {
            String[] locations = StringUtils.commaDelimitedListToStringArray(localLocations);
            builder.addPropertyValue("locations", locations);
        }
        builder.addPropertyValue(
                "location",
                parserContext.getDelegate().parseCustomElement(
                        DomUtils.getChildElementByTagName(element, "zkResource"), builder.getRawBeanDefinition()));
        builder.addPropertyValue("order", Integer.valueOf(order));
        builder.addPropertyValue("ignoreUnresolvablePlaceholders", true);
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
    }

    @Override
    protected Class<ZookeeperConfigurer> getBeanClass(Element element) {
        return ZookeeperConfigurer.class;
    }

}
