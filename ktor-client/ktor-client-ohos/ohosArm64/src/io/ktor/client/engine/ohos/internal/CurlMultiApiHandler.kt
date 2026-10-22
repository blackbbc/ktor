/*
* Copyright 2014-2021 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
*/

package io.ktor.client.engine.ohos.internal

import io.ktor.client.plugins.*
import io.ktor.utils.io.*
import io.ktor.utils.io.core.*
import io.ktor.utils.io.locks.*
import kotlinx.cinterop.*
import kotlinx.coroutines.*
import kotlinx.io.*
import libcurl.*

private class RequestHolder @OptIn(ExperimentalForeignApi::class) constructor(
    val responseCompletable: CompletableDeferred<CurlSuccess>,
    val requestWrapper: StableRef<CurlRequestBodyData>,
    val responseWrapper: StableRef<CurlResponseBodyData>,
    /** The native transfer is gone: whoever watches the request for cancellation can stop. */
    val onReleased: () -> Unit,
) {
    @OptIn(ExperimentalForeignApi::class)
    fun dispose() {
        requestWrapper.dispose()
        responseWrapper.dispose()
        onReleased()
    }
}

@OptIn(InternalAPI::class)
internal class CurlMultiApiHandler : Closeable {
    @OptIn(ExperimentalForeignApi::class)
    private val activeHandles = mutableMapOf<EasyHandle, RequestHolder>()

    /** [owner] tells the request apart from a later one whose easy handle reuses the same address. */
    @OptIn(ExperimentalForeignApi::class)
    private class Cancellation(
        val easyHandle: EasyHandle,
        val owner: CompletableDeferred<CurlSuccess>,
        val cause: Throwable,
    )

    /** Guards [pendingCancellations] and [closed]: cancellations arrive from any thread. */
    private val cancellationsLock = SynchronizedObject()
    private val pendingCancellations = mutableListOf<Cancellation>()
    private var closed = false

    @OptIn(ExperimentalForeignApi::class)
    private val multiHandle: MultiHandle = curl_multi_init()
        ?: throw RuntimeException("Could not initialize curl multi handle")

    private val easyHandlesToUnpauseLock = SynchronizedObject()

    @OptIn(ExperimentalForeignApi::class)
    private val easyHandlesToUnpause = mutableListOf<EasyHandle>()

    @OptIn(ExperimentalForeignApi::class)
    override fun close() {
        // No wakeup may reach the multi handle once it is cleaned up below.
        synchronized(cancellationsLock) {
            closed = true
            pendingCancellations.clear()
        }
        for ((handle, holder) in activeHandles) {
            cleanupEasyHandle(handle)
            holder.dispose()
        }

        activeHandles.clear()
        curl_multi_cleanup(multiHandle).verify()
    }

    @OptIn(ExperimentalForeignApi::class)
    fun scheduleRequest(
        request: CurlRequestData,
        deferred: CompletableDeferred<CurlSuccess>,
        onReleased: () -> Unit,
    ): EasyHandle {
        val easyHandle = curl_easy_init()
            ?: error("Could not initialize an easy handle")

        val bodyStartedReceiving = CompletableDeferred<Unit>()
        val responseData = CurlResponseBuilder(request)
        val responseDataRef = responseData.asStablePointer()

        val responseWrapper = CurlResponseBodyData(
            body = responseData.bodyChannel,
            callContext = request.executionContext,
            bodyStartedReceiving = bodyStartedReceiving,
            onUnpause = {
                synchronized(easyHandlesToUnpauseLock) {
                    easyHandlesToUnpause.add(easyHandle)
                }
                curl_multi_wakeup(multiHandle)
            }
        ).asStablePointer()

        bodyStartedReceiving.invokeOnCompletion {
            val result = collectSuccessResponse(easyHandle) ?: return@invokeOnCompletion
            activeHandles[easyHandle]!!.responseCompletable.complete(result)
        }

        setupMethod(easyHandle, request.method, request.contentLength)
        val requestWrapper = setupUploadContent(easyHandle, request)
        val requestHolder = RequestHolder(
            deferred,
            requestWrapper.asStableRef(),
            responseWrapper.asStableRef(),
            onReleased,
        )

        activeHandles[easyHandle] = requestHolder

        easyHandle.apply {
            option(CURLOPT_URL, request.url)
            option(CURLOPT_HTTPHEADER, request.headers)
            option(CURLOPT_HEADERFUNCTION, staticCFunction(::onHeadersReceived))
            option(CURLOPT_HEADERDATA, responseDataRef)
            option(CURLOPT_WRITEFUNCTION, staticCFunction(::onBodyChunkReceived))
            option(CURLOPT_WRITEDATA, responseWrapper)
            option(CURLOPT_PRIVATE, responseDataRef)
            option(CURLOPT_ACCEPT_ENCODING, "")
            request.connectTimeout?.let {
                if (it != HttpTimeoutConfig.INFINITE_TIMEOUT_MS) {
                    option(CURLOPT_CONNECTTIMEOUT_MS, request.connectTimeout)
                } else {
                    option(CURLOPT_CONNECTTIMEOUT_MS, Long.MAX_VALUE)
                }
            }

            request.proxy?.let { proxy ->
                option(CURLOPT_PROXY, proxy.toString())
                option(CURLOPT_SUPPRESS_CONNECT_HEADERS, 1L)
                if (request.forceProxyTunneling) {
                    option(CURLOPT_HTTPPROXYTUNNEL, 1L)
                }
            }

            if (!request.sslVerify) {
                option(CURLOPT_SSL_VERIFYPEER, 0L)
                option(CURLOPT_SSL_VERIFYHOST, 0L)
            }
            request.caPath?.let { option(CURLOPT_CAPATH, it) }
            request.caInfo?.let { option(CURLOPT_CAINFO, it) }
        }

        curl_multi_add_handle(multiHandle, easyHandle).verify()

        return easyHandle
    }

    /**
     * Thread-safe. The event loop never suspends while transfers are active, so a cancellation dispatched
     * to its thread would wait until every transfer finished; it is recorded here instead and the poll is
     * woken, so the next [perform] removes the transfer.
     */
    @OptIn(ExperimentalForeignApi::class)
    internal fun cancelRequest(easyHandle: EasyHandle, owner: CompletableDeferred<CurlSuccess>, cause: Throwable) {
        synchronized(cancellationsLock) {
            if (closed) return
            pendingCancellations += Cancellation(easyHandle, owner, cause)
            curl_multi_wakeup(multiHandle)
        }
    }

    private fun hasPendingCancellations(): Boolean = synchronized(cancellationsLock) { pendingCancellations.isNotEmpty() }

    /**
     * One round of the multi handle: perform, collect what finished, wait for activity, then return to
     * the event loop so it can schedule requests queued meanwhile.
     *
     * It used to loop until every transfer had finished, so a long-lived one (an SSE stream) kept every
     * request enqueued after it waiting in the queue until the stream closed. Backport of upstream
     * KTOR-8259 (ktorio/ktor@f6a545d89, 3.2.0). A queued request breaks the poll via [wakeup].
     *
     * Unlike upstream, finished transfers are collected before the poll: a response that completed in
     * this round would otherwise only see its body closed once the poll returned, up to its whole timeout
     * when the only other transfer is a quiet SSE stream.
     */
    @OptIn(ExperimentalForeignApi::class)
    internal fun perform(transfersRunning: IntVarOf<Int>) {
        if (activeHandles.isEmpty()) return

        // Process cancelled handles before performing so they don't hold up curl_multi_poll.
        if (hasPendingCancellations()) {
            handleCompleted()
        }

        if (activeHandles.isEmpty()) return

        synchronized(easyHandlesToUnpauseLock) {
            var handle = easyHandlesToUnpause.removeFirstOrNull()
            while (handle != null) {
                if (handle in activeHandles) curl_easy_pause(handle, CURLPAUSE_CONT)
                handle = easyHandlesToUnpause.removeFirstOrNull()
            }
        }
        curl_multi_perform(multiHandle, transfersRunning.ptr).verify()
        if (transfersRunning.value < activeHandles.size) {
            handleCompleted()
        }
        if (transfersRunning.value != 0) {
            curl_multi_poll(multiHandle, null, 0.toUInt(), 10000, null).verify()
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    internal fun hasHandlers(): Boolean = activeHandles.isNotEmpty()

    @OptIn(ExperimentalForeignApi::class)
    private fun setupMethod(
        easyHandle: EasyHandle,
        method: String,
        size: Long
    ) {
        easyHandle.apply {
            when (method) {
                "GET" -> option(CURLOPT_HTTPGET, 1L)
                "PUT" -> option(CURLOPT_PUT, 1L)
                "POST" -> {
                    option(CURLOPT_POST, 1L)
                    option(CURLOPT_POSTFIELDSIZE, size)
                }

                "HEAD" -> option(CURLOPT_NOBODY, 1L)
                else -> {
                    if (size > 0) option(CURLOPT_POST, 1L)
                    option(CURLOPT_CUSTOMREQUEST, method)
                }
            }
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun setupUploadContent(easyHandle: EasyHandle, request: CurlRequestData): COpaquePointer {
        val requestPointer = CurlRequestBodyData(
            body = request.content,
            callContext = request.executionContext,
            onUnpause = {
                synchronized(easyHandlesToUnpauseLock) {
                    easyHandlesToUnpause.add(easyHandle)
                }
                curl_multi_wakeup(multiHandle)
            }
        ).asStablePointer()

        easyHandle.apply {
            option(CURLOPT_READDATA, requestPointer)
            option(CURLOPT_READFUNCTION, staticCFunction(::onBodyChunkRequested))
            option(CURLOPT_INFILESIZE_LARGE, request.contentLength)
        }
        return requestPointer
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun handleCompleted() {
        // Process cancelled handles with safety guarantees
        val cancellations = synchronized(cancellationsLock) {
            pendingCancellations.toList().also { pendingCancellations.clear() }
        }
        for (cancellation in cancellations) {
            val easyHandle = cancellation.easyHandle
            val cause = cancellation.cause

            // Finished already, or the address now belongs to a newer request: nothing to cancel.
            if (activeHandles[easyHandle]?.responseCompletable !== cancellation.owner) continue
            val handler = activeHandles.remove(easyHandle) ?: continue

            try {
                val cancelled = processCancelledEasyHandle(easyHandle, cause, handler)
                handler.responseCompletable.completeExceptionally(cancelled.cause)
            } catch (e: Exception) {
                // Continue processing other handles
            } finally {
                handler.dispose()
            }
        }

        memScoped {
            do {
                val messagesLeft = alloc<IntVar>()
                val messagePtr = curl_multi_info_read(multiHandle, messagesLeft.ptr)
                val message = messagePtr?.pointed ?: continue

                val easyHandle = message.easy_handle
                    ?: error("Got a null easy handle from the message")

                try {
                    val result = processCompletedEasyHandle(message.msg, easyHandle, message.data.result)
                    val handler = activeHandles[easyHandle] ?: continue
                    
                    val deferred = handler.responseCompletable
                    if (deferred.isCompleted) continue
                    
                    when (result) {
                        is CurlSuccess -> deferred.complete(result)
                        is CurlFail -> deferred.completeExceptionally(result.cause)
                    }
                } finally {
                    activeHandles.remove(easyHandle)?.dispose()
                }
            } while (messagesLeft.value != 0)
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun processCancelledEasyHandle(
        easyHandle: EasyHandle,
        cause: Throwable,
        handler: RequestHolder
    ): CurlFail = memScoped {
        try {
            val responseDataRef = alloc<COpaquePointerVar>()
            easyHandle.apply { getInfo(CURLINFO_PRIVATE, responseDataRef.ptr) }
            val responseBuilder = responseDataRef.value!!.fromCPointer<CurlResponseBuilder>()
            try {
                return CurlFail(cause)
            } finally {
                responseBuilder.bodyChannel.close(cause)
                responseBuilder.headersBytes.close()
            }
        } finally {
            cleanupEasyHandle(easyHandle)
        }
    }

    /**
     * Processes a cancelled easy handle with safety guarantees against null pointer dereference.
     * 
     * This method uses the safe reference from [handler] instead of accessing through
     * CURLINFO_PRIVATE pointer, which may point to already freed memory in race conditions.
     * 
     * @param easyHandle The curl easy handle to process
     * @param cause The cancellation cause
     * @param handler The request holder containing safe references to response data
     * @return CurlFail containing the cancellation cause
     */
//    @OptIn(ExperimentalForeignApi::class)
//    private fun processCancelledEasyHandle(
//        easyHandle: EasyHandle,
//        cause: Throwable,
//        handler: RequestHolder
//    ): CurlFail {
//        try {
//            // Close bodyChannel using safe reference from handler
//            // This avoids dereferencing potentially freed CURLINFO_PRIVATE pointer
//            try {
//                val responseWrapper = handler.responseWrapper.get()
//                responseWrapper.body.close(cause)
//            } catch (e: Exception) {
//                // Non-fatal: bodyChannel may already be closed
//            }
//
//            // NOTE: We intentionally DO NOT close headersBytes here
//            // Reason: headersBytes is stored in CurlResponseBuilder, which is only accessible
//            // via CURLINFO_PRIVATE pointer. In cancelled request scenarios, this pointer may
//            // already point to freed memory due to race conditions, causing SIGSEGV crashes.
//            //
//            // The headersBytes (BytePacketBuilder) will be cleaned up by:
//            // 1. Normal garbage collection
//            // 2. Explicit cleanup in processCompletedEasyHandle for non-cancelled requests
//            //
//            // This trade-off prevents crashes at the cost of potentially delaying cleanup.
//            // The memory impact is minimal as BytePacketBuilder is lightweight.
//
//            return CurlFail(cause)
//        } finally {
//            cleanupEasyHandle(easyHandle)
//        }
//    }

    @OptIn(ExperimentalForeignApi::class)
    private fun processCompletedEasyHandle(
        message: CURLMSG?,
        easyHandle: EasyHandle,
        result: CURLcode
    ): CurlResponseData = memScoped {
        try {
            val responseDataRef = alloc<COpaquePointerVar>()
            val httpStatusCode = alloc<LongVar>()

            easyHandle.apply {
                getInfo(CURLINFO_RESPONSE_CODE, httpStatusCode.ptr)
                getInfo(CURLINFO_PRIVATE, responseDataRef.ptr)
            }

            val responseBuilder = responseDataRef.value!!.fromCPointer<CurlResponseBuilder>()
            try {
                collectFailedResponse(message, responseBuilder.request, result, httpStatusCode.value)
                    ?: collectSuccessResponse(easyHandle)!!
            } finally {
                responseBuilder.bodyChannel.close(null)
                responseBuilder.headersBytes.close()
            }
        } finally {
            cleanupEasyHandle(easyHandle)
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun collectFailedResponse(
        message: CURLMSG?,
        request: CurlRequestData,
        result: CURLcode,
        httpStatusCode: Long
    ): CurlFail? {
        curl_slist_free_all(request.headers)

        if (message != CURLMSG.CURLMSG_DONE) {
            return CurlFail(
                IllegalStateException("Request $request failed: $message")
            )
        }

        if (httpStatusCode != 0L) {
            return null
        }

        if (result == CURLE_OPERATION_TIMEDOUT) {
            return CurlFail(ConnectTimeoutException(request.url, request.connectTimeout))
        }

        val errorMessage = curl_easy_strerror(result)?.toKStringFromUtf8()

        if (result == CURLE_PEER_FAILED_VERIFICATION) {
            return CurlFail(
                IllegalStateException(
                    "TLS verification failed for request: $request. Reason: $errorMessage"
                )
            )
        }

        return CurlFail(
            IllegalStateException("Connection failed for request: $request. Reason: $errorMessage")
        )
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun collectSuccessResponse(easyHandle: EasyHandle): CurlSuccess? = memScoped {
        val responseDataRef = alloc<COpaquePointerVar>()
        val httpProtocolVersion = alloc<LongVar>()
        val httpStatusCode = alloc<LongVar>()

        easyHandle.apply {
            getInfo(CURLINFO_RESPONSE_CODE, httpStatusCode.ptr)
            getInfo(CURLINFO_PRIVATE, responseDataRef.ptr)
        }

        if (httpStatusCode.value == 0L) {
            // if error happened, it will be handled in collectCompleted
            return@memScoped null
        }

        val responseBuilder = responseDataRef.value!!.fromCPointer<CurlResponseBuilder>()
        with(responseBuilder) {
            val headers = headersBytes.build().readByteArray()

            CurlSuccess(
                httpStatusCode.value.toInt(),
                httpProtocolVersion.value.toUInt(),
                headers,
                bodyChannel
            )
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun cleanupEasyHandle(easyHandle: EasyHandle) {
        val removeResult = curl_multi_remove_handle(multiHandle, easyHandle)
        if (removeResult != CURLM_OK && removeResult != CURLM_BAD_EASY_HANDLE) {
            // Do not verify to avoid native exceptions during cleanup
            // removeResult.verify()
        }
        
        curl_easy_cleanup(easyHandle)
    }

    @OptIn(ExperimentalForeignApi::class)
    fun wakeup() {
        curl_multi_wakeup(multiHandle)
    }
}
