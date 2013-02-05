/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.vector;

import java.util.Random;

public abstract class ColumnVector {
	public boolean[] isNull; // if hasNulls is true, then this array contains true if the value is null, otherwise false.
		// The array is always allocated, so a batch can be re-used later and nulls added.
	public boolean noNulls; // if the whole column vector has no nulls, this is true, otherwise false.
	public boolean isRepeating; // true if same value repeats for whole column vector
	public boolean repeatingValueIsNull; // if the whole vector is null, this is true; undefined unless isRepeating==true
	public void setRandom(){assert false;}; // set column vector to all random data
	public void setSample(){assert false;}; // set column vector to sample data (a little faster)
	public void setRepeating(){assert false;}; // set column vector to a single repeating value
	public String getString(int i){return "ERROR: not implemented";} // get string representing the i'th element of vector
	
	public ColumnVector()
	{
		isNull = new boolean[VectorizedRowBatch.defaultSize];
		noNulls = true;
		isRepeating = false;
	}
	public void addRandomNulls() // sprinkle null values in this column vector
	{
		noNulls = false;
		Random rand = new Random();
		for(int i = 0; i!=isNull.length; i++)
		{
			isNull[i] = Math.abs( rand.nextInt() % 11 ) == 0;
		}
	}
	public void addSampleNulls() // add null values, but do it faster, by avoiding use of Random()
	{
		noNulls = false;
		assert isNull != null;
		for(int i = 0; i!=isNull.length; i++)
		{
			isNull[i] = i%11 == 0;
		}	
	}
}
 