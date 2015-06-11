package org.wzj.fmq.core;

/**
 * Created by wens on 15-6-11.
 */
public class Meta {

    private int dataStroeSegmentIndex ;
    private int dataStroeOffset ;

    public Meta(int dataStroeSegmentIndex, int dataStroeOffset) {
        this.dataStroeSegmentIndex = dataStroeSegmentIndex;
        this.dataStroeOffset = dataStroeOffset;
    }

    public int getDataStroeSegmentIndex() {
        return dataStroeSegmentIndex;
    }

    public void setDataStroeSegmentIndex(int dataStroeSegmentIndex) {
        this.dataStroeSegmentIndex = dataStroeSegmentIndex;
    }

    public int getDataStroeOffset() {
        return dataStroeOffset;
    }

    public void setDataStroeOffset(int dataStroeOffset) {
        this.dataStroeOffset = dataStroeOffset;
    }
}
