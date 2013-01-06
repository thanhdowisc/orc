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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A red-black tree that stores floats. The floats are stored as the integer
 * representation.
 */
class FloatRedBlackTree extends RedBlackTree {
  private final DynamicFloatArray keys = new DynamicFloatArray();
  private float newKey;

  public int add(float value) {
    newKey = value;
    // if the key is new, add it to our byteArray and store the offset & length
    if (add()) {
      keys.add(value);
    }
    return lastAdd;
  }

  @Override
  protected int compareValue(int position) {
    float other = keys.get(position);
    if (newKey > other) {
      return 1;
    } else {
      return newKey == other ? 0 : -1;
    }
  }

  /**
   * The information about each node.
   */
  public interface VisitorContext {
    /**
     * Get the position where the key was originally added.
     * @return the number returned by add.
     */
    int getOriginalPosition();

    /**
     * Get the original float.
     * @return the float
     */
    float getFloat();

    /**
     * Get the count for this key.
     * @return the number of times this key was added
     */
    int getCount();
  }

  /**
   * The interface for visitors.
   */
  public interface Visitor {
    /**
     * Called once for each node of the tree in sort order.
     * @param context the information about each node
     * @throws java.io.IOException
     */
    void visit(VisitorContext context) throws IOException;
  }

  private class VisitorContextImpl implements VisitorContext {
    private int originalPosition;
    private final Text text = new Text();

    public int getOriginalPosition() {
      return originalPosition;
    }

    public float getFloat() {
      return keys.get(originalPosition);
    }

    public int getCount() {
      return FloatRedBlackTree.this.getCount(originalPosition);
    }
  }

  private void recurse(int node, Visitor visitor, VisitorContextImpl context
                      ) throws IOException {
    if (node != NULL) {
      recurse(getLeft(node), visitor, context);
      context.originalPosition = node;
      visitor.visit(context);
      recurse(getRight(node), visitor, context);
    }
  }

  /**
   * Visit all of the nodes in the tree in sorted order.
   * @param visitor the action to be applied to each ndoe
   * @throws java.io.IOException
   */
  public void visit(Visitor visitor) throws IOException {
    recurse(root, visitor, new VisitorContextImpl());
  }

  /**
   * Reset the table to empty.
   */
  public void clear() {
    super.clear();
    keys.clear();
  }

  /**
   * Calculate the approximate size in memory.
   * @return the number of bytes used in storing the tree.
   */
  public long getByteSize() {
    return 4 * 4 * size();
  }
}
