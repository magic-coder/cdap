/**
 * Autogenerated by Thrift Compiler (0.7.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.continuuity.metadata.stubs;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * Defines a dataset types.
 */
public enum DatasetType implements org.apache.thrift.TEnum {
  BASIC(0),
  COUNTER(1),
  TIME_SERIES(2),
  CSV(3);

  private final int value;

  private DatasetType(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static DatasetType findByValue(int value) { 
    switch (value) {
      case 0:
        return BASIC;
      case 1:
        return COUNTER;
      case 2:
        return TIME_SERIES;
      case 3:
        return CSV;
      default:
        return null;
    }
  }
}
