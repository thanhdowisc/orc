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

/* This class represents a nullable double column vector. 
 * This class will be used for operations on all integer types (tinyint, smallint, int, bigint)
 * and as such will use a 64-bit long value to hold the biggest possible value. 
 * During copy-in/copy-out, smaller int types will be converted as needed. This will
 * reduce the amount of code that needs to be generated and also will run fast since the 
 * machine operates with 64-bit words.
 */
public class doubleColumnVector extends ColumnVector { 
	public double[] vector;
	public double repeatingValue; // if this is a repeated value (every value is the same for vector) then the value is stored here

	public doubleColumnVector(int len)
	{
		super();
		vector = new double[len];
	}
	
	@Override
	public String getString(int i)
	{
		if (noNulls || !isNull[i])
		{
			return Double.toString(vector[i]);
		}
		else
		{
			return "NULL";
		}
	}
	
	// Set the vector to sample data that repeats an iteration from 0 to 99.
	@Override
	public void setSample()
	{
		int size = vector.length;
		for(int i = 0; i!=size; i++)
		{
			vector[i] = i % 100; 
		}
	}

	// Set the vector to random data in the range 0 to 99.
	// This has significant overhead for random number generation. Use setSample() to reduce overhead.
	@Override
	public void setRandom()
	{
		int size = vector.length;
		Random rand = new Random();
		for(int i = 0; i!=size; i++)
		{
			vector[i] = Math.abs( rand.nextInt() % 100 );
		}
	}
	
	@Override
	public void setRepeating() {
		isRepeating = true;
		repeatingValue = 50.0;
	} 
}
