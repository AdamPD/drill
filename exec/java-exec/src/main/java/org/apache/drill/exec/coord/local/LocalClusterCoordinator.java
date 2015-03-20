/**
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
package org.apache.drill.exec.coord.local;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.coord.DistributedSemaphore;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.google.common.collect.Maps;

public class LocalClusterCoordinator extends ClusterCoordinator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LocalClusterCoordinator.class);

  /*
   * Since we hand out the endpoints list in {@see #getAvailableEndpoints()}, we use a
   * {@see java.util.concurrent.ConcurrentHashMap} because those guarantee not to throw
   * ConcurrentModificationException.
   */
  private final Map<RegistrationHandle, DrillbitEndpoint> endpoints = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, DistributedSemaphore> semaphores = Maps.newConcurrentMap();

  @Override
  public void close() throws IOException {
    endpoints.clear();
  }

  @Override
  public void start(final long millis) throws Exception {
    logger.debug("Local Cluster Coordinator started.");
  }

  @Override
  public RegistrationHandle register(final DrillbitEndpoint data) {
    logger.debug("Endpoint registered {}.", data);
    final Handle h = new Handle();
    endpoints.put(h, data);
    return h;
  }

  @Override
  public void unregister(final RegistrationHandle handle) {
    if (handle == null) {
      return;
    }

    endpoints.remove(handle);
  }

  @Override
  public Collection<DrillbitEndpoint> getAvailableEndpoints() {
    return endpoints.values();
  }

  private class Handle implements RegistrationHandle {
    private final UUID id = UUID.randomUUID();

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + getOuterType().hashCode();
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final Handle other = (Handle) obj;
      if (!getOuterType().equals(other.getOuterType())) {
        return false;
      }
      if (id == null) {
        if (other.id != null) {
          return false;
        }
      } else if (!id.equals(other.id)) {
        return false;
      }
      return true;
    }

    private LocalClusterCoordinator getOuterType() {
      return LocalClusterCoordinator.this;
    }
  }

  @Override
  public DistributedSemaphore getSemaphore(final String name, final int maximumLeases) {
    if (!semaphores.containsKey(name)) {
      semaphores.putIfAbsent(name, new LocalSemaphore(maximumLeases));
    }
    return semaphores.get(name);
  }

  public class LocalSemaphore implements DistributedSemaphore {
    private final Semaphore semaphore;
    private final LocalLease localLease = new LocalLease();

    public LocalSemaphore(final int size) {
      semaphore = new Semaphore(size);
    }

    @Override
    public DistributedLease acquire(final long timeout, final TimeUnit timeUnit) throws Exception {
      if (!semaphore.tryAcquire(timeout, timeUnit)) {
        return null;
      } else {
        return localLease;
      }
    }

    private class LocalLease implements DistributedLease {
      @Override
      public void close() throws Exception {
        semaphore.release();
      }
    }
  }
}
