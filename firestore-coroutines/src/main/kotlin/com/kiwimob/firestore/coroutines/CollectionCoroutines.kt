package com.kiwimob.firestore.coroutines

import com.google.firebase.firestore.CollectionReference
import com.google.firebase.firestore.DocumentReference
import com.google.firebase.firestore.DocumentSnapshot
import com.google.firebase.firestore.QuerySnapshot
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

suspend fun <T : Any> CollectionReference.await(clazz: Class<T>): List<T> {
    return await { documentSnapshot -> documentSnapshot.toObject(clazz) }
}

suspend fun <T : Any> CollectionReference.await(parser: (documentSnapshot: DocumentSnapshot) -> T?): List<T> {
    return suspendCancellableCoroutine { continuation ->
        get().addOnCompleteListener {
            if (it.isSuccessful && it.result != null) {
                val list = it.result!!.mapNotNull(parser)
                continuation.resume(list)
            } else {
                continuation.resumeWithException(it.exception ?: IllegalStateException())
            }
        }
    }
}

suspend fun CollectionReference.await() : QuerySnapshot {
    return suspendCancellableCoroutine { continuation ->
        get().addOnCompleteListener {
            if (it.isSuccessful && it.result != null) {
                continuation.resume(it.result!!)
            } else {
                continuation.resumeWithException(it.exception ?: IllegalStateException())
            }
        }
    }
}

suspend fun CollectionReference.addAwait(value: Any): DocumentReference {
    return suspendCancellableCoroutine { continuation ->
        add(value).addOnCompleteListener {
            if (it.isSuccessful && it.result != null) {
                continuation.resume(it.result!!)
            } else {
                continuation.resumeWithException(it.exception ?: IllegalStateException())
            }
        }
    }
}

suspend fun CollectionReference.addAwait(value: Map<String, Any>): DocumentReference {
    return suspendCancellableCoroutine { continuation ->
        add(value).addOnCompleteListener {
            if (it.isSuccessful && it.result != null) {
                continuation.resume(it.result!!)
            } else {
                continuation.resumeWithException(it.exception ?: IllegalStateException())
            }
        }
    }
}