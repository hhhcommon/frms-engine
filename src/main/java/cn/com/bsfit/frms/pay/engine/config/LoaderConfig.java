package cn.com.bsfit.frms.pay.engine.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import cn.com.bsfit.frms.base.config.FrmsConfigurable;
import cn.com.bsfit.frms.base.load.DataLoader;
import cn.com.bsfit.frms.engine.load.BasicDataLoader;
import cn.com.bsfit.frms.pay.engine.loader.dd.NoSqlDataLoader;

@Configuration
public class LoaderConfig implements FrmsConfigurable {

    @Autowired
    @Qualifier("ddNoSqlDataLoader")
    NoSqlDataLoader ddNoSqlDataLoader;

//    @Bean
//    RedisKryoPipelStorer redisKryoPipelStorer() {
//        return new RedisKryoPipelStorer();
//    }

    @Bean(name="ddNoSqlDataLoader")
    NoSqlDataLoader ddNoSqlDataLoader() {
    	NoSqlDataLoader loader = new NoSqlDataLoader();
        return loader;
    }
    
    @Bean
    BasicDataLoader basicDataLoader() {
        List<DataLoader> dataLoaderList = new ArrayList<>(1);
        dataLoaderList.add(ddNoSqlDataLoader);
        BasicDataLoader basicDataLoader = new BasicDataLoader();
        basicDataLoader.setDataLoaders(dataLoaderList);
        return basicDataLoader;
    }

}
