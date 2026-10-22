/*
 * Copyright 2014-2019 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.client.engine.ohos

import io.ktor.client.engine.ohos.internal.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.atomicfu.*
import kotlinx.cinterop.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.coroutines.*

internal class RequestContainer(
    val requestData: CurlRequestData,
    val completionHandler: CompletableDeferred<CurlSuccess>
)

internal class CurlProcessor(coroutineContext: CoroutineContext) {
    @OptIn(InternalAPI::class)
    private val curlDispatcher: CloseableCoroutineDispatcher =
        Dispatchers.createFixedThreadDispatcher("curl-dispatcher", 1)

    private var curlApi: CurlMultiApiHandler? by atomic(null)
    private val closed = atomic(false)

    private val curlScope = CoroutineScope(coroutineContext + curlDispatcher)
    private val requestQueue: Channel<RequestContainer> = Channel(Channel.UNLIMITED)

    init {
        val init = curlScope.launch {
            curlApi = CurlMultiApiHandler()
        }

        runBlocking {
            init.join()
        }

        runEventLoop()
    }

    suspend fun executeRequest(request: CurlRequestData): CurlSuccess {
        val result = CompletableDeferred<CurlSuccess>()
        requestQueue.send(RequestContainer(request, result))
        curlApi!!.wakeup()
        return result.await()
    }

    @OptIn(DelicateCoroutinesApi::class, ExperimentalForeignApi::class)
    private fun runEventLoop() {
        curlScope.launch {
            memScoped {
                val transfersRunning = alloc<IntVar>()
                val api = curlApi!!

                while (!requestQueue.isClosedForReceive) {
                    try {
                        drainRequestQueue(api)
                        api.perform(transfersRunning)
                    } catch (e: kotlinx.coroutines.CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        // Continue event loop on error
                    }
                }
            }
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private suspend fun drainRequestQueue(api: CurlMultiApiHandler) {
        while (true) {
            val container = if (api.hasHandlers()) {
                requestQueue.tryReceive()
            } else {
                requestQueue.receiveCatching()
            }.getOrNull() ?: break

            // Watched until the native transfer is released, not just until the response starts: a streamed
            // body (SSE) keeps its transfer long after that, and cancelling the call must still remove it.
            // Release happens on this thread in a later perform(), so the handle is always set by then.
            var requestCleaner: DisposableHandle? = null
            val requestHandler = api.scheduleRequest(container.requestData, container.completionHandler) {
                requestCleaner?.dispose()
            }

            requestCleaner = container.requestData.executionContext.invokeOnCompletion { cause ->
                if (cause == null) return@invokeOnCompletion
                cancelRequest(requestHandler, container.completionHandler, cause)
            }
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    fun close() {
        if (!closed.compareAndSet(false, true)) return

        requestQueue.close()
        GlobalScope.launch(curlDispatcher) {
            curlScope.coroutineContext[Job]!!.join()
            curlApi!!.close()
        }.invokeOnCompletion {
            curlDispatcher.close()
        }
    }

    /** Straight to the handler, not through [curlScope]: the event loop never yields while transfers run. */
    @OptIn(ExperimentalForeignApi::class)
    private fun cancelRequest(easyHandle: EasyHandle, owner: CompletableDeferred<CurlSuccess>, cause: Throwable) {
        curlApi!!.cancelRequest(easyHandle, owner, cause)
    }
}
