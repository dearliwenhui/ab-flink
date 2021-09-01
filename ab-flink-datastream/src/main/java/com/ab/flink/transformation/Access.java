package com.ab.flink.transformation;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-01 17:12
 **/
public class Access {
    private Long time;
    private String domain;
    private Long traffic;

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public Long getTraffic() {
        return traffic;
    }

    public void setTraffic(Long traffic) {
        this.traffic = traffic;
    }

    public Access() {
    }

    public Access(Long time, String domain, Long traffic) {
        this.time = time;
        this.domain = domain;
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "Access{" +
                "time=" + time +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }
}
