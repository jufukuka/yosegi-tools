#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

basedir=$(cd $(dirname $0); pwd)

libdir=$(cd "$basedir/../jars"; pwd)
if [ ! -d "$libdir" ]; then
  echo "$libdir is not found." >&2
  echo "Please setup \"$basedir/setup.sh \" " >&2
  exit 255
fi

function java_exec() {
  local JAVA_CMD="$JAVA_HOME/bin/java"
  if [ ! -e $JAVA_CMD ]; then JAVA_CMD=java; fi

  local HEAP_SIZE=${HEAP_SIZE:-512m}
  local JAVA_OPTS=${JAVA_OPTS:-}

  local dn
  local lib_paths=($libdir/lib $libdir)
  lib_paths+=($(
    find $libdir/yosegi -type d \
    | grep -v version \
    | sed -e :loop -e 'N; $!b loop' -e 's/\n/ /g' \
  ))

  local class_paths='.'
  for dn in ${lib_paths[@]}
  do
    class_paths="$class_paths:$dn/*"
  done
  $JAVA_CMD $JAVA_OPTS --add-opens=java.base/java.nio=ALL-UNNAMED -Xmx$HEAP_SIZE -Xms$HEAP_SIZE -cp "$class_paths" jp.co.yahoo.yosegi.tools.YosegiTool $*
}

function show_usage() {
  echo "setup  setup yosegi lib dir." >&2
  java_exec help
  exit $1
}

case "$1" in
  ""      ) show_usage 255 ;;
  "setup" ) shift; $basedir/setup.sh $*; exit $? ;;
  "help"  ) show_usage 0 ;;
  "-h"    ) show_usage 0 ;;
  * ) java_exec $*
esac

