/*
 * Copyright 2019-2022 Mamoe Technologies and contributors.
 *
 * 此源代码的使用受 GNU AFFERO GENERAL PUBLIC LICENSE version 3 许可证的约束, 可以在以下链接找到该许可证.
 * Use of this source code is governed by the GNU AGPLv3 license that can be found through the following link.
 *
 * https://github.com/mamoe/mirai/blob/dev/LICENSE
 */

@file:JvmMultifileClass
@file:JvmName("MiraiUtils")
@file:Suppress("NOTHING_TO_INLINE")

package net.mamoe.mirai.utils

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.suspendCancellableCoroutine
import java.util.*
import java.util.Spliterators.AbstractSpliterator
import java.util.concurrent.ArrayBlockingQueue
import java.util.function.Consumer
import java.util.stream.Stream
import java.util.stream.StreamSupport
import kotlin.coroutines.*
import kotlin.coroutines.intrinsics.COROUTINE_SUSPENDED
import kotlin.coroutines.intrinsics.createCoroutineUnintercepted
import kotlin.coroutines.intrinsics.suspendCoroutineUninterceptedOrReturn
import kotlin.streams.asStream

@JvmSynthetic
public inline fun <T> stream(@BuilderInference noinline block: suspend SequenceScope<T>.() -> Unit): Stream<T> =
    sequence(block).asStream()

@Suppress("RemoveExplicitTypeArguments")
public object JdkStreamSupport {
    private class CompleteToken(val error: Throwable?) {
        override fun toString(): String {
            return "CompleteToken[$error]"
        }
    }

    private val NULL_PLACEHOLDER = Symbol("null")!!

    /*
    Implementation:

    Spliterator.tryAdvance():
        - Resume coroutine
        (*Wait for collector.emit*)
        - Re-Suspend flow
        - Put response to queue

        - Fire response to jdk consumer

    Completion & Exception caught:
        (* Spliterator.tryAdvance(): Resume coroutine *)
        (* No more values or exception thrown. *)
        (* completion called *)
        - Put the exception or the completion token to queue

        - Throw exception in Spliterator.tryAdvance() if possible
        - Return false in Spliterator.tryAdvance()

     */
    public fun <T> Flow<T>.toStream(
        context: CoroutineContext = EmptyCoroutineContext,
        unintercepted: Boolean = true,
    ): Stream<T> {
        val spliterator = FlowSpliterator(
            flow = this,
            coroutineContext = context,
            unintercepted = unintercepted,
        )

        return StreamSupport.stream(spliterator, false).onClose {
            spliterator.cancelled = true
            spliterator.nextStep?.resumeWithException(CancellationException())
            spliterator.nextStep = null
        }
    }

    private class FlowSpliterator<T>(
        private val flow: Flow<T>,
        private val coroutineContext: CoroutineContext,
        unintercepted: Boolean
    ) : AbstractSpliterator<T>(
        Long.MAX_VALUE, Spliterator.ORDERED or Spliterator.IMMUTABLE
    ) {

        private val queue = ArrayBlockingQueue<Any?>(4)
        private var completed = false

        @JvmField
        var cancelled = false

        @JvmField
        var nextStep: Continuation<Unit>? = run {
            val completion = object : Continuation<Unit> {
                override val context: CoroutineContext get() = coroutineContext

                override fun resumeWith(result: Result<Unit>) {
                    nextStep = null
                    queue.put(CompleteToken(result.exceptionOrNull()))
                    completed = true
                }

            }
            if (unintercepted) {
                return@run (suspend {
                    flow.collect { item ->
                        suspendCoroutineUninterceptedOrReturn<Unit> { cort ->
                            nextStep = cort
                            queue.put(boxValue(item))
                            COROUTINE_SUSPENDED
                        }
                    }
                }).createCoroutineUnintercepted(completion)
            }

            return@run (suspend {
                flow.collect { item ->
                    suspendCancellableCoroutine<Unit> { cort ->
                        nextStep = cort
                        queue.put(boxValue(item))
                    }
                }
            }).createCoroutine(completion)
        }

        private inline fun boxValue(value: Any?): Any {
            return value ?: NULL_PLACEHOLDER
        }

        private fun complete(value: Any?, action: Consumer<in T>): Boolean {
            if (value is CompleteToken) {
                value.error?.let { throw boxError(it) }
                completed = true
                return false
            }
            if (value === NULL_PLACEHOLDER) { // null
                @Suppress("UNCHECKED_CAST")
                action.accept(null as T)
                return true
            }
            @Suppress("UNCHECKED_CAST")
            action.accept(value as T)
            return true
        }

        override fun tryAdvance(action: Consumer<in T>): Boolean {
            if (completed) return false

            if (queue.isNotEmpty()) {
                return complete(queue.take(), action)
            }
            if (cancelled) return false

            val step = nextStep!!
            nextStep = null
            step.resume(Unit)

            return complete(queue.take(), action)
        }

    }

    private val FSClassName: String = FlowSpliterator::class.java.name

    private fun boxError(error: Throwable): Throwable {
        if (error is Error) return error // System error. Nothing to do

        error.stackTrace.forEach { stackTraceElement ->
            if (stackTraceElement.className == FSClassName && stackTraceElement.methodName == "tryAdvance") {
                // Stack trace OK
                return error
            }
        }

        // Stack traces of Spliterator.tryAdvance() lose
        return RuntimeException(error)
    }
}

