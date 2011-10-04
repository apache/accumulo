package org.apache.accumulo.server.test.randomwalk.bulk;

import java.util.Arrays;
import java.util.Random;

import org.apache.accumulo.server.test.randomwalk.State;
import org.apache.hadoop.io.Text;


public class Merge extends BulkTest {

    
    @Override
    protected void runLater(State state) throws Exception {
        Text[] points = getRandomTabletRange(state);
        log.info("merging " + rangeToString(points)); 
        state.getConnector().tableOperations().merge(Setup.getTableName(), points[0], points[1]);
        log.info("merging " + rangeToString(points) + " complete"); 
    }
    
    public static String rangeToString(Text[] points) {
        return "(" + (points[0] == null ? "-inf" : points[0]) + 
               " -> " + (points[1] == null ? "+inf" : points[1]) + "]";
    }
    
    public static Text getRandomRow(Random rand) {
        return new Text(String.format(BulkPlusOne.FMT, Math.abs(rand.nextLong()) % BulkPlusOne.LOTS));
    }

    public static Text[] getRandomTabletRange(State state) {
        Random rand = (Random)state.get("rand");
        Text points[] = {
                getRandomRow(rand),
                getRandomRow(rand),
        };
        Arrays.sort(points);
        if (rand.nextInt(10) == 0) {
            points[0] = null;
        }
        if (rand.nextInt(10) == 0) {
            points[1] = null;
        }
        if (rand.nextInt(20) == 0) {
            points[0] = null;
            points[1] = null;
        }
        return points;
    }

}
