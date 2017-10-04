package com.basic.core.model;

import java.io.Serializable;

/**
 * locate com.basic.model
 * Created by 79875 on 2017/10/4.
 */
public class NodePair implements Serializable {
    private String upnode;
    private String downnode;

    public NodePair() {
    }

    public NodePair(String upnode, String downnode) {
        this.upnode = upnode;
        this.downnode = downnode;
    }

    public String getUpnode() {
        return upnode;
    }

    public void setUpnode(String upnode) {
        this.upnode = upnode;
    }

    public String getDownnode() {
        return downnode;
    }

    public void setDownnode(String downnode) {
        this.downnode = downnode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodePair nodePair = (NodePair) o;

        if (!upnode.equals(nodePair.upnode)) return false;
        return downnode.equals(nodePair.downnode);
    }

    @Override
    public int hashCode() {
        int result = upnode.hashCode();
        result = 31 * result + downnode.hashCode();
        return result;
    }
}
