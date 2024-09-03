package com.empayre.liminator.dao;

import com.empayre.liminator.domain.tables.pojos.LimitContext;

public interface LimitContextDao extends CommonDao<LimitContext> {

    LimitContext getLimitContext(Long limitId);
}
