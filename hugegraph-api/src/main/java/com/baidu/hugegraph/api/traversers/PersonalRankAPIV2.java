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

package com.baidu.hugegraph.api.traversers;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.PersonalRankTraverser;
import com.baidu.hugegraph.traversal.algorithm.PersonalRankTraverserV2;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.*;

@Path("graphs/{graph}/traversers/personalrankv2")
@Singleton
public class PersonalRankAPIV2 extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgumentNotNull(request.sources ,
                        "The source vertex id of rank request can't be null");
        E.checkArgument(request.label != null,
                        "The edge label of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.degree > 0 || request.degree == NO_LIMIT,
                        "The degree of rank request must be > 0, but got: %s",
                        request.degree);
        E.checkArgument(request.limit > 0 || request.limit == NO_LIMIT,
                        "The limit of rank request must be > 0, but got: %s",
                        request.limit);
        E.checkArgument(request.maxDepth > 0 &&
                        request.maxDepth <= Long.valueOf(DEFAULT_MAX_DEPTH),
                        "The max depth of rank request must be " +
                        "in range (0, %s], but got '%s'",
                        DEFAULT_MAX_DEPTH, request.maxDepth);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                  "edge label '{}', alpha '{}', degree '{}', " +
                  "max depth '{}' and sorted '{}'",
                  graph, request.sources, request.label, request.alpha,
                  request.degree, request.maxDepth, request.sorted);

        List<Id> ids = new ArrayList<>();
        String[] split = request.sources.split(",");
        for (String id : split){
            Id sourceId = HugeVertex.getIdValue(id);
            ids.add(sourceId);
        }

        HugeGraph g = graph(manager, graph);

        PersonalRankTraverserV2 traverser;
        traverser = new PersonalRankTraverserV2(g, request.alpha, request.degree,
                                              request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(ids, request.label,
                Directions.OUT,request.withLabel);
        ranks = HugeTraverser.topN(ranks, request.sorted, request.limit);
        return manager.serializer(g).writeMap(ranks);
    }

    private static class RankRequest {

        @JsonProperty("sources")
        private String sources;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("degree")
        private long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("limit")
        private long limit = Long.valueOf(DEFAULT_LIMIT);
        @JsonProperty("max_depth")
        private int maxDepth;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
                PersonalRankTraverser.WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s,alpha=%s," +
                                 "degree=%s,limit=%s,maxDepth=%s," +
                                 "withLabel=%s,sorted=%s}",
                                 this.sources, this.label, this.alpha,
                                 this.degree, this.limit, this.maxDepth,
                                 this.withLabel, this.sorted);
        }
    }
}
