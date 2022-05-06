// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlemanagedprometheus

import (
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/collector/internal/datapointstorage"
	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/collector/internal/normalization"
)

// GMPNormalizer performs normalization on cumulative points which:
//  (a) don't have a start time, OR
//  (b) have been sent a preceding "reset" point as described in https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
// The first point without a start time is cached, and is NOT exported.
// Subsequent points "subtract" the initial point prior to exporting.
// GMP Treats reset points (where the value of the point has decreased)
// differently from the standard normalizer, which normalizes subsequent points
// against them. Reset points in the GMP Normalizer have the start time set to
// T.reset - 1 ms, and are sent without modifying the value. Subsequent points'
// values are not adjusted, but do use the adjusted start time (T.reset - 1 ms).
func GMPNormalizer(shutdown <-chan struct{}, logger *zap.Logger) normalization.Normalizer {
	return &normalizer{
		cache: datapointstorage.NewCache(shutdown),
		log:   logger,
	}
}

type normalizer struct {
	cache datapointstorage.Cache
	log   *zap.Logger
}

func (n *normalizer) NormalizeExponentialHistogramDataPoint(point pmetric.ExponentialHistogramDataPoint, identifier string) *pmetric.ExponentialHistogramDataPoint {
	// Drop exponential histogram points and log.
	n.log.Debug("google managed prometheus doesn't support exponential histograms. Dropping the point")
	return nil
}

func (n *normalizer) NormalizeHistogramDataPoint(point pmetric.HistogramDataPoint, identifier string) *pmetric.HistogramDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	// normalize points without a start time.
	if point.StartTimestamp() == 0 {
		start, ok := n.cache.GetHistogramDataPoint(identifier)
		if !ok {
			// This is the first time we've seen this metric. Record it in history
			// and drop the point.
			n.cache.SetHistogramDataPoint(identifier, &point)
			return nil
		}
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			n.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		// Make a copy so we don't mutate underlying data
		newPoint := pmetric.NewHistogramDataPoint()
		point.CopyTo(newPoint)
		// Use the start timestamp from the normalization point
		newPoint.SetStartTimestamp(start.Timestamp())
		// Adjust the value based on the start point's value
		newPoint.SetCount(point.Count() - start.Count())
		// We drop points without a sum, so no need to check here.
		newPoint.SetSum(point.Sum() - start.Sum())
		pointBuckets := point.BucketCounts()
		startBuckets := start.BucketCounts()
		if !bucketBoundariesEqual(point.ExplicitBounds(), start.ExplicitBounds()) {
			// The number of buckets changed, so we can't normalize points anymore.
			// Treat this as a reset by recording and dropping this point.
			n.cache.SetHistogramDataPoint(identifier, &point)
			return nil
		}
		newBuckets := make([]uint64, len(pointBuckets))
		for i := range pointBuckets {
			newBuckets[i] = pointBuckets[i] - startBuckets[i]
		}
		newPoint.SetBucketCounts(newBuckets)
		normalizedPoint = normalization.SubtractHistogramDataPoint(&point, start)
	}
	// GMP-specific handling
	if !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// Handle explicit reset points.
		// Make a copy so we don't mutate underlying data.
		newPoint := pmetric.NewHistogramDataPoint()
		point.CopyTo(newPoint)
		// StartTime = Timestamp - 1 ms
		newPoint.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(time.Millisecond)))
		return &newPoint
	}
	return normalizedPoint
}

func bucketBoundariesEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// NormalizeNumberDataPoint normalizes a cumulative, monotonic sum.
// It returns the normalized point, or nil if the point should be dropped.
func (n *normalizer) NormalizeNumberDataPoint(point pmetric.NumberDataPoint, identifier string) *pmetric.NumberDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := n.cache.GetNumberDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			n.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		// Make a copy so we don't mutate underlying data
		newPoint := pmetric.NewNumberDataPoint()
		point.CopyTo(newPoint)
		// Use the start timestamp from the normalization point
		newPoint.SetStartTimestamp(start.Timestamp())
		// Adjust the value based on the start point's value
		switch newPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			newPoint.SetIntVal(point.IntVal() - start.IntVal())
		case pmetric.NumberDataPointValueTypeDouble:
			newPoint.SetDoubleVal(point.DoubleVal() - start.DoubleVal())
		}
		normalizedPoint = &newPoint
	}
	if !ok && point.StartTimestamp() == 0 {
		// This is the first time we've seen this point.
		// Record it in history and drop the point.
		n.cache.SetNumberDataPoint(identifier, &point)
		return nil
	}
	// GMP-specific handling
	if !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// we received an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// This indicates the value decreased.
		// Make a copy so we don't mutate underlying data
		newPoint := pmetric.NewNumberDataPoint()
		point.CopyTo(newPoint)
		// The start timestamp is T - 1ms
		newPoint.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(time.Millisecond)))
		normalizedPoint = &newPoint

		// We don't want to subtract a value from subsequent points.
		// Create and store a point with the start/end of the new point, but with 0 value
		zeroPoint := pmetric.NewNumberDataPoint()
		zeroPoint.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(time.Millisecond)))
		zeroPoint.SetTimestamp(newPoint.Timestamp())
		// Set the value to zero
		switch newPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			zeroPoint.SetDoubleVal(0.0)
		case pmetric.NumberDataPointValueTypeInt:
			zeroPoint.SetIntVal(0)
		}
		n.cache.SetNumberDataPoint(identifier, &zeroPoint)
	}
	return normalizedPoint
}

func (n *normalizer) NormalizeSummaryDataPoint(point pmetric.SummaryDataPoint, identifier string) *pmetric.SummaryDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := n.cache.GetSummaryDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			n.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		// Make a copy so we don't mutate underlying data.
		newPoint := pmetric.NewSummaryDataPoint()
		// Quantile values are copied, and are not modified. Quantiles are
		// computed over the same time period as sum and count, but it isn't
		// possible to normalize them.
		point.CopyTo(newPoint)
		// Use the start timestamp from the normalization point
		newPoint.SetStartTimestamp(start.Timestamp())
		// Adjust the value based on the start point's value
		newPoint.SetCount(point.Count() - start.Count())
		// We drop points without a sum, so no need to check here.
		newPoint.SetSum(point.Sum() - start.Sum())
		normalizedPoint = &newPoint
	}
	if !ok && point.StartTimestamp() == 0 {
		// This is the first time we've seen this metric, or we received
		// an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// Record it in history and drop the point.
		n.cache.SetSummaryDataPoint(identifier, &point)
		return nil
	}
	// GMP-specific handling
	if !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// we received an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// This indicates the value decreased.
		// Make a copy so we don't mutate underlying data
		newPoint := pmetric.NewSummaryDataPoint()
		point.CopyTo(newPoint)
		// The start timestamp is T - 1ms
		newPoint.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(time.Millisecond)))
		normalizedPoint = &newPoint

		// We don't want to subtract a value from subsequent points.
		// Create and store a point with the start/end of the new point, but with 0 value
		zeroPoint := pmetric.NewSummaryDataPoint()
		zeroPoint.SetStartTimestamp(pcommon.Timestamp(uint64(point.Timestamp()) - uint64(time.Millisecond)))
		zeroPoint.SetTimestamp(newPoint.Timestamp())
		// sum and count default to 0
		n.cache.SetSummaryDataPoint(identifier, &zeroPoint)
	}
	return normalizedPoint
}
