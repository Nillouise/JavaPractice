package aifunai.dao;

import aifunai.model.TestTableWithBLOBs;

public interface TestTableMapper {
    int insert(TestTableWithBLOBs record);

    int insertSelective(TestTableWithBLOBs record);
}