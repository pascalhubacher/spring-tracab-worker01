package com.mas2022datascience.springtracabworker01.processor;

import com.mas2022datascience.avro.v1.Frame;
import com.mas2022datascience.avro.v1.Object;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

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

  public Frame getVelocity(Frame actualFrame) {

    // velocity cannot be calculated
    if (historyList.size() == 0) {
      return actualFrame;
    }

    Frame oldFrame = historyList.get(historyList.size());

    // add new element and remove oldest element
    if (!historyList.get(historyList.size()).equals(actualFrame)) {
      historyList.add(actualFrame);
    }
    if (historyList.size() == 3) {
      historyList.remove(0);
    }

    return calcVelocity(actualFrame, oldFrame);
  }

  public Frame getAcceleration(Frame actualFrame) {

    return actualFrame;
  }

  /**
   * calculate the euclidian distance between two points in a 3 dimensional vector space
   * @param element1
   * @param element2
   * @return
   */
  private double euclidianDistance(Object element1, Object element2) {
    return Math.sqrt(
        Math.pow(element1.getX()-element2.getX(), 2) + Math.pow(element1.getY()-element2.getY(), 2)
            + Math.pow(element1.getZ()-element2.getZ(), 2)
    );
  }

  /**
   * calculates the time delta in microseconds
   * @param element1
   * @param element2
   * @return
   */
  private double calcDeltaTime(Frame element1, Frame element2) {

    return 1.0;
  }

//  #calculates the time delta in microseconds
//  def calcDeltaTime(timestampNew, timestampOld):
//      #timestamps must be formatted as ISO8601 string Timestamp
//    # ts -> deltatime in microseconds
//    return((datetime.strptime(str(timestampNew), '%Y.%m.%dT%H:%M:%S.%f')-datetime.strptime(str(timestampOld), '%Y.%m.%dT%H:%M:%S.%f')).microseconds)
//

  /**
   * calculates the velocity
   * math: velocity = delta distance [m] / delta time [s] (linear velocity)
   * @param actualFrame
   * @param oldFrame
   * @return
   */
  private Frame calcVelocity(Frame actualFrame, Frame oldFrame) {

    Long actualTime = utcString2epocMs(actualFrame.getUtc());
    Long oldTime = utcString2epocMs(oldFrame.getUtc());

    List<Object> actualObjects = actualFrame.getObjects();
    List<Object> oldObjects = oldFrame.getObjects();

    actualObjects.stream().forEach( object -> {
      //TODO calculate velocity and acceleration of object
      }
    );
    actualFrame.setObjects(actualObjects);
    return actualFrame;
  }

  //      #calculates the velocity -> math: velocity = delta distance [m] / delta time [s] (linear velocity)
//  def calcVelocity(pointNew, pointOld):
//      #point1 and 2 must be formatted as a set like this (x, y, z, ts)
//      #timeDelta is in microseconds
//  timeDelta = calcDeltaTime(pointNew[3],pointOld[3])/1000/1000
//      #timedelta must be bigger than 0
//      if float(timeDelta) > 0:
//      return(euclidianDistance((pointNew[0], pointNew[1], pointNew[2]), (pointOld[0], pointOld[1], pointOld[2]))/(timeDelta))
//      else:
//      return('NaN')

  /**
   *
   * @param element1
   * @param element2
   * @return
   */
  private Frame calcAcceleration(Frame element1, Frame element2) {

    return element1;
  }

  //      #calculates the acceleration -> math: acceleration = delta velocity [m/s]/ delta time [s] (linear acceleration)
//  def calcAcceleration(advancedInfoNew, advancedInfoOld):
//      #point1 and 2 must be formatted as a set like this
//      #ts [str], velocity [float], acceleration[float], distance[str], directionVector[set], id[int], matchid[int])
//
//      #returns float [m/s^2]
//      return((advancedInfoNew.velocity - advancedInfoOld.velocity) / (calcDeltaTime(advancedInfoNew.ts, advancedInfoOld.ts)/1000/1000))

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
