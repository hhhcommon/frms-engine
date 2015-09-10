/**
 * 
 */
package cn.com.bsfit.frms.engine.mock;

import org.drools.agent.KnowledgeAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import cn.com.bsfit.frms.base.config.FrmsConfigurable;
import cn.com.bsfit.frms.base.config.KnowledgeAgentConfiguration;

/**
 * @author 王新根
 * @since 3.0.0
 * 
 */
public class MockConfig extends KnowledgeAgentConfiguration implements FrmsConfigurable {
    Logger logger = LoggerFactory.getLogger(MockConfig.class);

    @Override
    public String[] getLocalResources() {
        return new String[] { "classpath:rules/frmsInternal.drl", "classpath:rules/test.drl" };
    }

    @Bean
    @Primary
    public KnowledgeAgent defaultKnowledgeAgent() throws Exception {
        return super.getAgent();
    }
}
