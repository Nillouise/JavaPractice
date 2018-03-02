package aifunai.model;

public class TestTableWithBLOBs extends TestTable {
    private String parentId;

    private String vertexId;

    public String getParentId() {
        return parentId;
    }

    public void setParentId(String parentId) {
        this.parentId = parentId == null ? null : parentId.trim();
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId == null ? null : vertexId.trim();
    }
}