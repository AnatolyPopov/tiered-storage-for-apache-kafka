/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage;

import java.util.Map;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

/**
 * SPI contract for the optional Iceberg module. Discovered via {@link java.util.ServiceLoader}.
 * Keeping this interface in core means core has zero compile-time dependency on Iceberg.
 */
public interface IcebergManagerFactory {
    InternalRemoteStorageManager create(Logger log, Time time, Map<String, ?> configs);
}
