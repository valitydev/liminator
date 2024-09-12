package com.empayre.liminator.dao;

import lombok.Getter;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;

@Getter
public abstract class AbstractDao {

    private final DSLContext dslContext;

    public AbstractDao(DataSourceConnectionProvider dataSource) {
        Configuration configuration = new DefaultConfiguration();
        configuration.set(dataSource);
        configuration.set(SQLDialect.POSTGRES);
        this.dslContext = DSL.using(configuration);
    }
}
