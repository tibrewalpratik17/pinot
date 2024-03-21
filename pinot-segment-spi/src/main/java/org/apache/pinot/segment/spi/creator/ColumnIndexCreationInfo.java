/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.spi.creator;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.partition.PartitionFunction;


public class ColumnIndexCreationInfo implements Serializable {
  private final boolean _createDictionary;
  private final boolean _useVarLengthDictionary;
  private final boolean _isAutoGenerated;
  private final Object _defaultNullValue;
  private final ColumnStatistics _columnStatistics;

  public ColumnIndexCreationInfo(ColumnStatistics columnStatistics, boolean createDictionary,
      boolean useVarLengthDictionary, boolean isAutoGenerated, Object defaultNullValue) {
    _columnStatistics = columnStatistics;
    _createDictionary = createDictionary;
    _useVarLengthDictionary = useVarLengthDictionary;
    _isAutoGenerated = isAutoGenerated;
    _defaultNullValue = defaultNullValue;
  }

  public ColumnStatistics getColumnStatistics() {
    return _columnStatistics;
  }

  public boolean isCreateDictionary() {
    return _createDictionary;
  }

  public boolean isUseVarLengthDictionary() {
    return _useVarLengthDictionary;
  }

  public Object getMin() {
    return _columnStatistics.getMinValue();
  }

  public Object getMax() {
    return _columnStatistics.getMaxValue();
  }

  public Object getSortedUniqueElementsArray() {
    return _columnStatistics.getUniqueValuesSet();
  }

  public int getDistinctValueCount() {
    Object uniqueValArray = _columnStatistics.getUniqueValuesSet();
    if (uniqueValArray == null) {
      return Constants.UNKNOWN_CARDINALITY;
    }
    return ArrayUtils.getLength(uniqueValArray);
  }

  public boolean isSorted() {
    return _columnStatistics.isSorted();
  }

  public int getTotalNumberOfEntries() {
    return _columnStatistics.getTotalNumberOfEntries();
  }

  public int getMaxNumberOfMultiValueElements() {
    return _columnStatistics.getMaxNumberOfMultiValues();
  }

  public int getMaxRowLengthInBytes() {
    return _columnStatistics.getMaxRowLengthInBytes();
  }

  public boolean isAutoGenerated() {
    return _isAutoGenerated;
  }

  public Object getDefaultNullValue() {
    return _defaultNullValue;
  }

  public int getLengthOfLongestEntry() {
    return _columnStatistics.getLengthOfLargestElement();
  }

  public Set<Integer> getPartitions() {
    return _columnStatistics.getPartitions();
  }

  public PartitionFunction getPartitionFunction() {
    return _columnStatistics.getPartitionFunction();
  }

  public int getNumPartitions() {
    return _columnStatistics.getNumPartitions();
  }

  public boolean isFixedLength() {
    return _columnStatistics.isFixedLength();
  }

  @Nullable
  public Map<String, String> getPartitionFunctionConfig() {
    return _columnStatistics.getPartitionFunctionConfig();
  }
}
