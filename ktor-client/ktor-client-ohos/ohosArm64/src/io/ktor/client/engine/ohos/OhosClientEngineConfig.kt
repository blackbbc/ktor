/*
 * Copyright 2014-2025 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.client.engine.ohos

import io.ktor.client.engine.HttpClientEngineConfig

public class OhosClientEngineConfig: HttpClientEngineConfig() {
    /**
     * Use HarmonyOS native RCP (Remote Communication Protocol) API instead of curl.
     * 
     * RCP is the recommended approach for HarmonyOS applications as it provides
     * better integration with the platform and avoids potential curl-related issues.
     * 
     * Set to `false` to use curl as a fallback.
     *
     * Default: `false`
     */
    public var useRcp: Boolean = false

    /**
     * Forces proxy tunneling by setting `CURLOPT_HTTPPROXYTUNNEL`.
     */
    internal var forceProxyTunneling: Boolean = false

    /**
     * Sets path to Certificate Authority (CA) bundle using `CURLOPT_CAINFO`.
     */
    public var caInfo: String? = null

    /**
     * Sets directory that holds Certificate Authority (CA) certificates using `CURLOPT_CAPATH`.
     */
    public var caPath: String? = null

    /**
     * Enables TLS host and certificate verification by setting the
     * `CURLOPT_SSL_VERIFYPEER` and `CURLOPT_SSL_VERIFYHOST` options.
     * Similar to `-k/--insecure` curl option.
     *
     * Setting this property to `false` is recommended only for testing purposes.
     */
    public var sslVerify: Boolean = true
}
