#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

source /etc/profile
source ~/.bash_profile

CUBE_NAME=$1
INTERVAL=$2
DELAY=$3
MARGIN=$4
AUTHORIZATION=$5
KYLIN_HOST=$6
CURRENT_TIME_IN_SECOND=`date +%s`
CURRENT_TIME=$((CURRENT_TIME_IN_SECOND * 1000))
START=$(($CURRENT_TIME - CURRENT_TIME%INTERVAL - DELAY))
END=$(($CURRENT_TIME - CURRENT_TIME%INTERVAL - DELAY + INTERVAL))

ID="$START"_"$END"
echo "building for ${CUBE_NAME} ${ID}" >> ${KYLIN_HOME}/logs/build_trace.log
curl --request PUT --data "{\"start\": $START, \"end\": $END }" --header "Authorization: Basic $AUTHORIZATION" --header "Content-Type: application/json" -v ${KYLIN_HOST}/kylin/api/streaming/${CUBE_NAME}/build
