package com.mas2022datascience.springtracabworker01.processor;

import com.mas2022datascience.avro.v1.Frame;
import com.mas2022datascience.avro.v1.Object;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 *
 */
public class singletonList {

  private static singletonList instance;

  final private ArrayList<Frame> historyList = new ArrayList<Frame>();

  private singletonList() {
    // private constructor
  }

  //synchronized method to control simultaneous access
  synchronized public static singletonList getInstance() {
    if (instance == null) {
      // if instance is null, initialize
      instance = new singletonList();
    }
    return instance;
  }

  public Frame calculate(Frame actualFrame) {

    // velocity cannot be calculated
    if (historyList.size() == 0) {
      historyList.add(actualFrame);
      return actualFrame;
    }

    Frame oldFrame = historyList.get(historyList.size()-1);

    List<Object> actualObjects = actualFrame.getObjects();
    HashMap<String, Object> oldObjectsMap = new HashMap<>();
    oldFrame.getObjects().forEach( object ->
        oldObjectsMap.put(object.getId(), object)
    );

    // update velocity and acceleration to actual object
    actualObjects.stream().forEach( actualObject -> {
      if (oldObjectsMap.containsKey(actualObject.getId())) {
        actualObject.setVelocity(calcVelocity(actualObject,
            actualFrame.getUtc(),
            oldObjectsMap.get(actualObject.getId()),
            oldFrame.getUtc()).orElse(null));
        actualObject.setAccelleration(calcAcceleration(actualObject,
            actualFrame.getUtc(),
            oldObjectsMap.get(actualObject.getId()),
            oldFrame.getUtc()).orElse(null));
      }
    });

    // add new element and remove oldest element
    if (!historyList.get(historyList.size()-1).equals(actualFrame)) {
      historyList.add(actualFrame);
    }
    if (historyList.size() > 2) {
      historyList.remove(0);
    }

    return actualFrame;
  }

  /**
   * calculates the euclidian distance between two points in a 3 dimensional space
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @return distance between 2 points in a 3 dimensional space
   */
  private double getEuclidianDistance(Object actualObject, Object oldObject) {
    return Math.sqrt(
        Math.pow(oldObject.getX()-actualObject.getX(), 2) + Math.pow(oldObject.getY()-actualObject.getY(), 2)
            + Math.pow(oldObject.getZ()-actualObject.getZ(), 2)
    );
  }

  /**
   * calculates the time difference in microseconds
   * @param actualUtc UTC time as a String
   * @param oldUtc UTC time as a String
   * @return time difference in microseconds
   */
  private double getTimeDifference(String actualUtc, String oldUtc) {
    return utcString2epocMs(actualUtc) - utcString2epocMs(oldUtc);
  }

  /**
   * calculates the velocity between two points
   * math: velocity = delta distance [m] / delta time [s] (linear velocity)
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return
   */
  private Optional<Double> calcVelocity(Object actualObject, String actualUtc, Object oldObject, String oldUtc) {
    Double timeDifference = getTimeDifference(actualUtc, oldUtc);
    Double distanceDifference = getEuclidianDistance(actualObject, oldObject);

    // represents the divisor that is needed to get m. Ex. cm to m means 100 as 100cm is 1m
    int distanceUnitDivisor = 100;
    // represents the divisor that is needed to get s. Ex. ms to s means 1000 as 1000ms is 1s
    int timeUnitDivisor = 1000;

    if (timeDifference == 0) {
      return Optional.of(null);
    } else {
      return Optional.of((distanceDifference / distanceUnitDivisor) / (timeDifference / timeUnitDivisor));
    }
  }

  /**
   * calculates the acceleration between to points
   * math: acceleration = delta velocity [m/s]/ delta time [s] (linear acceleration)
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return
   */
  private Optional<Double> calcAcceleration(Object actualObject, String actualUtc, Object oldObject, String oldUtc) {
    Double timeDifference = getTimeDifference(actualUtc, oldUtc);

    // represents the divisor that is needed to get s. Ex. ms to s means 1000 as 1000ms is 1s
    int timeUnitDivisor = 1000;

    if (oldObject.getVelocity() == null || actualObject.getVelocity() == null || timeDifference == 0 ) {
      return null;
    } else {
      Double velocityDifference = actualObject.getVelocity() - oldObject.getVelocity();
      return Optional.of(velocityDifference / (timeDifference / timeUnitDivisor));
    }
  }

  /**
   * Convertes the utc string of type "yyyy-MM-dd'T'HH:mm:ss.SSS" to epoc time in milliseconds.
   * @param utcString
   * @return epoc time in milliseconds
   */
  private static long utcString2epocMs(String utcString) {
    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC);

    return Instant.from(fmt.parse(utcString)).toEpochMilli();
  }

}
