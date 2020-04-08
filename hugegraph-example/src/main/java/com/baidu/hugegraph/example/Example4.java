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

package com.baidu.hugegraph.example;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.PersonalRankTraverser;
import com.baidu.hugegraph.traversal.algorithm.PersonalRankTraverserV2;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Example4 {

    private static final Logger LOG = Log.logger(Example4.class);

    private HugeGraph graph;

    @Before
    public void beforeTest(){
        graph = ExampleUtil.loadGraph();
        Example3.loadPersonalRankData(graph);
    }

    @After
    public void beforeAfter(){
        graph.close();
        HugeGraph.shutdown(30L);
    }


    @Test
    public void oriTest(){
        PersonalRankTraverser traverser0 = new PersonalRankTraverser(graph, 0.4, 1000, 10);
        Id sourceId = HugeVertex.getIdValue("A");
        Map<Id, Double> ranks = traverser0.personalRank(sourceId, "like", PersonalRankTraverser.WithLabel.BOTH_LABEL);
        System.out.println(ranks);
    }
    @Test
    public void newTest(){
        PersonalRankTraverserV2 traverser0 = new PersonalRankTraverserV2(graph, 0.4, 1000, 10);
        List<Id> sources = new ArrayList<>();
        sources.add(HugeVertex.getIdValue("A"));
        Map<Id, Double> ranks = traverser0.personalRank(sources, "like", Directions.OUT, PersonalRankTraverser.WithLabel.BOTH_LABEL);
        System.out.println(ranks);
    }

    @Test
    public void batchTest(){
        PersonalRankTraverserV2 traverser = new PersonalRankTraverserV2(graph, 0.4, 1000, 5);
        List<Id> sources = new ArrayList<>();
        sources.add(HugeVertex.getIdValue("A"));
        sources.add(HugeVertex.getIdValue("B"));
        Map<Id, Double> ranks2 = traverser.personalRank(sources, "like", Directions.OUT, PersonalRankTraverser.WithLabel.BOTH_LABEL);
        System.out.println(ranks2);
    }

}
