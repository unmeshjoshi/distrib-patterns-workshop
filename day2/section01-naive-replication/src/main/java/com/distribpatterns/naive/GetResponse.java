package com.distribpatterns.naive;

/**
 * Response to get request
 * Serialization is handled by tickloom's Message infrastructure
 */
public record GetResponse(boolean found, int value) {
}

