package com.pdn.apitest.beans;

public class Pattern {
    public String action1;
    public String action2;

    public Pattern() {
    }

    public Pattern(String action1, String action2) {
        this.action1 = action1;
        this.action2 = action2;
    }

    public String getAction1() {
        return action1;
    }

    public void setAction1(String action1) {
        this.action1 = action1;
    }

    public String getAction2() {
        return action2;
    }

    public void setAction2(String action2) {
        this.action2 = action2;
    }

    @Override
    public String toString() {
        return "Pattern{" +
                "action1='" + action1 + '\'' +
                ", action2='" + action2 + '\'' +
                '}';
    }
}
