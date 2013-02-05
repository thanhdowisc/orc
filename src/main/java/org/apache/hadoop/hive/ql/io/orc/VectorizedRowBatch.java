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

public class VectorizedRowBatch {
	// A VectorizedRowBatch is a set of n rows, organized with each column
	// as a vector. It is the unit of query execution, organized to minimize
	// the cost per row and achieve high cycles-per-instruction. 
	public int numCols; // number of columns
	public ColumnVector[] cols; // a vector for each column
	public int size; // number of rows that qualify (i.e. haven't been filtered out)
	public int selected[]; // array of positions of selected values
	public boolean selectedInUse; // if no filtering has been applied yet, selectedInUse is false,
					// meaning that all rows qualify. If it is true, then the selected[] array
					// records the offsets of qualifying rows.
	public boolean EOF; // if this is true, then there is no data in the batch -- we have hit the end of input
	public static final int defaultSize = 1024; // This number is carefully chosen to minimize overhead
							// and typically allows one VectorizedRowBatch to fit in L1 cache.
	
	public VectorizedRowBatch (int numCols)
	{
		this.numCols = numCols;
		this.size = -1; // set to an illegal value so we notice it fast if we don't initialize this batch later
		selected = new int[defaultSize];
		selectedInUse = false;
		this.cols = new ColumnVector[numCols];
	}

	public void setRandom() {
		size = defaultSize;
		for (int i = 0; i != numCols; i++)
		{
			cols[i] = new longColumnVector(defaultSize);
			cols[i].setRandom();
		}
	}
	
	public void setSample() {
		size = defaultSize;
		for (int i = 0; i != numCols; i++)
		{
			cols[i].setSample();
		}
	}
	
	// set to sample data, re-using existing columns in batch
	public void setSampleOverwrite() {
		// put sample data in the columns
		for (int i = 0; i != numCols; i++)
		{
			cols[i].setSample();
		}
		// reset the selection vector
		selectedInUse = false;
		size = defaultSize;
	}

	// return count of qualifying rows
	public long count() {
		return size; // size is the number of remaining rows whether or not rows have been filtered out
	}
	
	// print all the rows that still qualify
	public void printSelected()
	{
		if (selected == null) // print every row
		{
			for (int i = 0; i != size; i++)
			{
				printRow(i);
			}
		}
		else // print rows indicated by offsets in selected array
		{
			for (int j = 0; j != size; j++)
			{
				int i = selected[j];
				printRow(i);
			}
		}
	}
	private void printRow(int i)
	{
		for(int j = 0; j!=numCols; j++)
		{
			System.out.print(cols[j].getString(i));
			System.out.print(" ");
		}
		System.out.print("\n");
	}

	public void addRandomNulls() {
		for (int i = 0; i != numCols; i++)
		{
			cols[i].addRandomNulls();
		}		
	}

	public void addSampleNulls() {
		for (int i = 0; i != numCols; i++)
		{
			cols[i].addSampleNulls();
		}
	}

	public void setRepeating() {
		for (int i = 0; i != numCols; i++)
		{
			cols[i].setRepeating();
		}
	}
}
