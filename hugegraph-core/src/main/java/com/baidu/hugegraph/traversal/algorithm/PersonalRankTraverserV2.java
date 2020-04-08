/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.traversal.algorithm;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;

import java.util.*;

public class PersonalRankTraverserV2 extends PersonalRankTraverser {

    public PersonalRankTraverserV2(HugeGraph graph, double alpha,
                                   long degree, int maxDepth) {
        super(graph,alpha,degree,maxDepth);
    }

    //从相同的节点游走
    public Map<Id, Double> personalRank(List<Id> sources, String label, Directions dir,
                                        WithLabel withLabel) {
        E.checkNotEmpty(sources, "The sources vertex id can't be empty");
        E.checkArgumentNotNull(label, "The edge label can't be null");

        Map<Id, Double> ranks = new HashMap<>();
        for (Id source : sources ) {
            ranks.put(source, 1.0);
        }

        Id labelId = this.graph().edgeLabel(label).id();

        Set<Id> outSeeds = new HashSet<>();
        Set<Id> inSeeds = new HashSet<>();
        if (dir == Directions.OUT) {
            outSeeds.addAll(sources);
        } else {
            inSeeds.addAll(sources);
        }

        Set<Id> rootAdjacencies = new HashSet<>();
        for (long i = 0; i < super.maxDepth; i++) {
            Map<Id, Double> newRanks = this.calcNewRanks(outSeeds, inSeeds,
                                                         labelId, ranks);
            ranks = this.compensateRoot(sources, newRanks);
            if (i == 0) {
                rootAdjacencies.addAll(ranks.keySet());
            }
        }
//         Remove directly connected neighbors
        removeAll(ranks, rootAdjacencies);
//         Remove unnecessary label
        if (withLabel == WithLabel.SAME_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? inSeeds : outSeeds);
        } else if (withLabel == WithLabel.OTHER_LABEL) {
            removeAll(ranks, dir == Directions.OUT ? outSeeds : inSeeds);
        }
        return ranks;
    }

    public Map<Id, Double> compensateRoot(List<Id> roots, Map<Id, Double> newRanks) {
        for (Id root : roots) {
            double rank = newRanks.getOrDefault(root, 0.0);
            rank += (1 - this.alpha);
            newRanks.put(root, rank);
        }
        return newRanks;
    }




}
