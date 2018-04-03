package com.basic.core.model;

import org.apache.storm.scheduler.resource.Component;

import java.io.Serializable;

/**
 * locate com.basic.model
 * Created by 79875 on 2017/10/4.
 */
public class ComponentPair implements Serializable{
    private Component upcomponent;
    private Component downcomponent;

    public ComponentPair() {
    }

    public ComponentPair(Component upcomponent, Component downcomponent) {
        this.upcomponent = upcomponent;
        this.downcomponent = downcomponent;
    }

    public Component getUpcomponent() {
        return upcomponent;
    }

    public void setUpcomponent(Component upcomponent) {
        this.upcomponent = upcomponent;
    }

    public Component getdowncomponent() {
        return downcomponent;
    }

    public void setdowncomponent(Component downcomponent) {
        this.downcomponent = downcomponent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ComponentPair that = (ComponentPair) o;

        if (!upcomponent.id.equals(that.upcomponent.id)) return false;
        return downcomponent.id.equals(that.downcomponent.id);
    }

    @Override
    public int hashCode() {
        int result = upcomponent.id.hashCode();
        result = 31 * result + downcomponent.id.hashCode();
        return result;
    }
}
