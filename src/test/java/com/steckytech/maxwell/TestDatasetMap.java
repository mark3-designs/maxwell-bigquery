package com.steckytech.maxwell;

import com.stekytech.maxwell.DatasetMap;
import org.junit.Assert;
import org.junit.Test;

public class TestDatasetMap {


    @Test
    public void testDatasetMap() {

        DatasetMap map = new DatasetMap("projectid");

        map.add("ncr_flight_dev", "ncrocketry_dev");
        map.add("flight_dev", "ncrocketry_dev");
        map.add("flight", "ncrocketry");


        Assert.assertEquals("ncrocketry_dev", map.datasetFor("ncr_flight_dev"));
        Assert.assertEquals("ncrocketry_dev", map.datasetFor("flight_dev"));
        Assert.assertEquals("ncrocketry", map.datasetFor("flight"));
        Assert.assertEquals("something_else", map.datasetFor("something_else"));

    }


}
