/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

/**
 * Key group range assignment for a KeyedStream.
 */
@Internal
public final class KeyGroupRangeAssignment {

	/**
	 * The default lower bound for max parallelism if nothing was configured by the user. We have this so allow users
	 * some degree of scale-up in case they forgot to configure maximum parallelism explicitly.
	 */
	public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = 1 << 7;

	/** The (inclusive) upper bound for max parallelism. */
	public static final int UPPER_BOUND_MAX_PARALLELISM = Transformation.UPPER_BOUND_MAX_PARALLELISM;

	private KeyGroupRangeAssignment() {
		throw new AssertionError();
	}

	/**
	 * Assigns the given key to a parallel operator index.
	 *
	 * @param key            the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @param parallelism    the current parallelism of the operator
	 * @return the index of the parallel operator to which the given key should be routed.
	 */
	public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism) {
		Preconditions.checkNotNull(key, "Assigned key must not be null!");
		return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism));
	}

	/*
	public static int assignKeyToParallelOperator(Object key, int maxParallelism, int parallelism, long hops) {
		return computeOperatorIndexForKeyGroup(maxParallelism, parallelism, assignToKeyGroup(key, maxParallelism, hops));
	}
	*/

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param key            the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int assignToKeyGroup(Object key, int maxParallelism) {
		Preconditions.checkNotNull(key, "Assigned key must not be null!");
		return computeKeyGroupForKeyHash(key.hashCode(), maxParallelism);
	}

	// public static int assignToKeyGroup(Object key, int maxParallelism, long hops) { return computeKeyGroupForKeyHashPartial(key.hashCode(), maxParallelism, hops); }

	/**
	 * Assigns the given key to a key-group index.
	 *
	 * @param keyHash        the hash of the key to assign
	 * @param maxParallelism the maximum supported parallelism, aka the number of key-groups.
	 * @return the key-group to which the given key is assigned
	 */
	public static int computeKeyGroupForKeyHash(int keyHash, int maxParallelism) {
		return MathUtils.murmurHash(keyHash) % maxParallelism;
	}

	/*
	public static int computeKeyGroupForKeyHashPartial(int keyHash, int maxParallelism, long hops) {
		int keyGroup = MathUtils.murmurHash(keyHash) % maxParallelism;
		int newKeyGroup = 0;
		if (hops == 0) {
			newKeyGroup = keyGroup;
		} else if (hops > 0 && (keyGroup + 32 < maxParallelism)) {
			newKeyGroup = keyGroup + 32;
		} else if (hops > 0 && (keyGroup + 32 > maxParallelism)) {
			newKeyGroup = keyGroup - 32;
		}
		System.err.println("key[" + keyHash + "] hops[" + hops + "] keyGroup[" + keyGroup + "] newKeyGroup[" + newKeyGroup + "]");
		return keyGroup;
	}
	*/

	/**
	 * Computes the range of key-groups that are assigned to a given operator under the given parallelism and maximum
	 * parallelism.
<<<<<<< HEAD
	 * <p>
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
=======
	 *
	 * <p>IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
>>>>>>> 2806215c0c4d88c336ee57ab639d24685b485273
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 * </p>
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param operatorIndex  Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return the computed key-group range for the operator.
	 */
	public static KeyGroupRange computeKeyGroupRangeForOperatorIndex(
		int maxParallelism,
		int parallelism,
		int operatorIndex) {

		checkParallelismPreconditions(parallelism);
		checkParallelismPreconditions(maxParallelism);

		Preconditions.checkArgument(maxParallelism >= parallelism,
			"Maximum parallelism must not be smaller than parallelism.");

		int start = ((operatorIndex * maxParallelism + parallelism - 1) / parallelism);
		int end = ((operatorIndex + 1) * maxParallelism - 1) / parallelism;
		return new KeyGroupRange(start, end);
	}

	/**
	 * Computes the index of the operator to which a key-group belongs under the given parallelism and maximum
	 * parallelism.
<<<<<<< HEAD
	 * <p>
	 * IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
=======
	 *
	 * <p>IMPORTANT: maxParallelism must be <= Short.MAX_VALUE to avoid rounding problems in this method. If we ever want
>>>>>>> 2806215c0c4d88c336ee57ab639d24685b485273
	 * to go beyond this boundary, this method must perform arithmetic on long values.
	 * </p>
	 *
	 * @param maxParallelism Maximal parallelism that the job was initially created with.
	 *                       0 < parallelism <= maxParallelism <= Short.MAX_VALUE must hold.
	 * @param parallelism    The current parallelism under which the job runs. Must be <= maxParallelism.
	 * @param keyGroupId     Id of a key-group. 0 <= keyGroupID < maxParallelism.
	 * @return The index of the operator to which elements from the given key-group should be routed under the given
	 * parallelism and maxParallelism.
	 */
	public static int computeOperatorIndexForKeyGroup(int maxParallelism, int parallelism, int keyGroupId) {
		return keyGroupId * parallelism / maxParallelism;
	}

	/**
	 * Computes a default maximum parallelism from the operator parallelism. This is used in case the user has not
	 * explicitly configured a maximum parallelism to still allow a certain degree of scale-up.
	 *
	 * @param operatorParallelism the operator parallelism as basis for computation.
	 * @return the computed default maximum parallelism.
	 */
	public static int computeDefaultMaxParallelism(int operatorParallelism) {

		checkParallelismPreconditions(operatorParallelism);

		return Math.min(
			Math.max(
				MathUtils.roundUpToPowerOfTwo(operatorParallelism + (operatorParallelism / 2)),
				DEFAULT_LOWER_BOUND_MAX_PARALLELISM),
			UPPER_BOUND_MAX_PARALLELISM);
	}

	public static void checkParallelismPreconditions(int parallelism) {
		Preconditions.checkArgument(parallelism > 0
				&& parallelism <= UPPER_BOUND_MAX_PARALLELISM,
			"Operator parallelism not within bounds: " + parallelism);
	}
}
