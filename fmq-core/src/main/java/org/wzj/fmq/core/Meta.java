package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-11.
 */
public class Meta {

    private int dataStroeSegment ;
    private int dataStroeOffset ;

    public Meta(int dataStroeSegment, int dataStroeOffset) {
        this.dataStroeSegment = dataStroeSegment;
        this.dataStroeOffset = dataStroeOffset;
    }

    public int getDataStroeSegment() {
        return dataStroeSegment;
    }

    public void setDataStroeSegment(int dataStroeSegment) {
        this.dataStroeSegment = dataStroeSegment;
    }

    public int getDataStroeOffset() {
        return dataStroeOffset;
    }

    public void setDataStroeOffset(int dataStroeOffset) {
        this.dataStroeOffset = dataStroeOffset;
    }
}
