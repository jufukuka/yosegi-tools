/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.co.yahoo.yosegi.tools.logging.server.header;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;

public class HeaderProcess {

  private final List<String> keys = new ArrayList<String>();
  private final Map<String,IHeaderPlugin> plungins = new HashMap<String,IHeaderPlugin>();

  public void put( final String name , final IHeaderPlugin pluging ){
    keys.add( name );
    plungins.put( name , pluging );
  }

  public List<String> getKeyList() {
    return keys;
  }

  public HeaderResult get( final HttpServletRequest req ) throws IOException {
    HeaderResult result = new HeaderResult();
    for ( String key : keys ) {
      result.put( key , plungins.get( key ).get( req ) );
    }
    return result;
  }

}
