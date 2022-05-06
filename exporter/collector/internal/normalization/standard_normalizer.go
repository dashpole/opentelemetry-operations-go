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

package normalization

import (
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/collector/internal/datapointstorage"
)

// NewStandardNormalizer performs normalization on cumulative points which:
//  (a) don't have a start time, OR
//  (b) have been sent a preceding "reset" point as described in https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
// The first point without a start time or the reset point is cached, and is
// NOT exported. Subsequent points "subtract" the initial point prior to exporting.
func NewStandardNormalizer(shutdown <-chan struct{}, logger *zap.Logger) Normalizer {
	return &standardNormalizer{
		cache: datapointstorage.NewCache(shutdown),
		log:   logger,
	}
}

type standardNormalizer struct {
	cache datapointstorage.Cache
	log   *zap.Logger
}

func (s *standardNormalizer) NormalizeExponentialHistogramDataPoint(point pmetric.ExponentialHistogramDataPoint, identifier string) *pmetric.ExponentialHistogramDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := s.cache.GetExponentialHistogramDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			s.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		if point.Scale() != start.Scale() {
			// TODO(#366): It is possible, but difficult to compare exponential
			// histograms with different scales. For now, treat a change in
			// scale as a reset.
			s.cache.SetExponentialHistogramDataPoint(identifier, &point)
			return nil
		}
		if point.Count() < start.Count() {
			// The value reset, so store the current
			s.cache.SetExponentialHistogramDataPoint(identifier, &point)
			return nil
		}
		normalizedPoint = SubtractExponentialHistogramDataPoint(&point, start)
	}
	if (!ok && point.StartTimestamp() == 0) || !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// This is the first time we've seen this metric, or we received
		// an explicit reset point as described in
		//
		// Record it in history and drop the point.
		s.cache.SetExponentialHistogramDataPoint(identifier, &point)
		return nil
	}
	return normalizedPoint
}

// SubtractExponentialHistogramDataPoint subtracts b from a.
func SubtractExponentialHistogramDataPoint(a, b *pmetric.ExponentialHistogramDataPoint) *pmetric.ExponentialHistogramDataPoint {
	// Make a copy so we don't mutate underlying data
	newPoint := pmetric.NewExponentialHistogramDataPoint()
	a.CopyTo(newPoint)
	// Use the start timestamp from the normalization point
	newPoint.SetStartTimestamp(b.Timestamp())
	// Adjust the value based on the start point's value
	newPoint.SetCount(a.Count() - b.Count())
	// We drop points without a sum, so no need to check here.
	newPoint.SetSum(a.Sum() - b.Sum())
	newPoint.SetZeroCount(a.ZeroCount() - b.ZeroCount())
	normalizeExponentialBuckets(newPoint.Positive(), b.Positive())
	normalizeExponentialBuckets(newPoint.Negative(), b.Negative())
	return &newPoint
}

func normalizeExponentialBuckets(pointBuckets, startBuckets pmetric.Buckets) {
	newBuckets := make([]uint64, len(pointBuckets.MBucketCounts()))
	offsetDiff := int(pointBuckets.Offset() - startBuckets.Offset())
	for i := range pointBuckets.MBucketCounts() {
		startOffset := i + offsetDiff
		// if there is no corresponding bucket for the starting MBucketCounts, don't normalize
		if startOffset < 0 || startOffset >= len(startBuckets.MBucketCounts()) {
			newBuckets[i] = pointBuckets.MBucketCounts()[i]
		} else {
			newBuckets[i] = pointBuckets.MBucketCounts()[i] - startBuckets.MBucketCounts()[startOffset]
		}
	}
	pointBuckets.SetOffset(pointBuckets.Offset())
	pointBuckets.SetMBucketCounts(newBuckets)
}

func (s *standardNormalizer) NormalizeHistogramDataPoint(point pmetric.HistogramDataPoint, identifier string) *pmetric.HistogramDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := s.cache.GetHistogramDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			s.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		if !bucketBoundariesEqual(point.MExplicitBounds(), start.MExplicitBounds()) {
			// The number of buckets changed, so we can't normalize points anymore.
			// Treat this as a reset by recording and dropping this point.
			s.cache.SetHistogramDataPoint(identifier, &point)
			return nil
		}
		normalizedPoint = SubtractHistogramDataPoint(&point, start)
	}
	if (!ok && point.StartTimestamp() == 0) || !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// This is the first time we've seen this metric, or we received
		// an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// Record it in history and drop the point.
		s.cache.SetHistogramDataPoint(identifier, &point)
		return nil
	}
	return normalizedPoint
}

// SubtractHistogramDataPoint subtracts b from a.
func SubtractHistogramDataPoint(a, b *pmetric.HistogramDataPoint) *pmetric.HistogramDataPoint {
	// Make a copy so we don't mutate underlying data
	newPoint := pmetric.NewHistogramDataPoint()
	a.CopyTo(newPoint)
	// Use the start timestamp from the normalization point
	newPoint.SetStartTimestamp(b.Timestamp())
	// Adjust the value based on the start point's value
	newPoint.SetCount(a.Count() - b.Count())
	// We drop points without a sum, so no need to check here.
	newPoint.SetSum(a.Sum() - b.Sum())
	pointBuckets := a.MBucketCounts()
	startBuckets := b.MBucketCounts()
	newBuckets := make([]uint64, len(pointBuckets))
	for i := range pointBuckets {
		newBuckets[i] = pointBuckets[i] - startBuckets[i]
	}
	newPoint.SetMBucketCounts(newBuckets)
	return &newPoint
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
func (s *standardNormalizer) NormalizeNumberDataPoint(point pmetric.NumberDataPoint, identifier string) *pmetric.NumberDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := s.cache.GetNumberDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			s.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		normalizedPoint = SubtractNumberDataPoint(&point, start)
	}
	if (!ok && point.StartTimestamp() == 0) || !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// This is the first time we've seen this metric, or we received
		// an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// Record it in history and drop the point.
		s.cache.SetNumberDataPoint(identifier, &point)
		return nil
	}
	return normalizedPoint
}

// SubtractNumberDataPoint subtracts b from a.
func SubtractNumberDataPoint(a, b *pmetric.NumberDataPoint) *pmetric.NumberDataPoint {
	// Make a copy so we don't mutate underlying data
	newPoint := pmetric.NewNumberDataPoint()
	a.CopyTo(newPoint)
	// Use the start timestamp from the normalization point
	newPoint.SetStartTimestamp(b.Timestamp())
	// Adjust the value based on the start point's value
	switch newPoint.ValueType() {
	case pmetric.NumberDataPointValueTypeInt:
		newPoint.SetIntVal(a.IntVal() - b.IntVal())
	case pmetric.NumberDataPointValueTypeDouble:
		newPoint.SetDoubleVal(a.DoubleVal() - b.DoubleVal())
	}
	return &newPoint
}

func (s *standardNormalizer) NormalizeSummaryDataPoint(point pmetric.SummaryDataPoint, identifier string) *pmetric.SummaryDataPoint {
	// if the point doesn't need to be normalized, use original point
	normalizedPoint := &point
	start, ok := s.cache.GetSummaryDataPoint(identifier)
	if ok {
		if !start.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
			// We found a cached start timestamp that wouldn't produce a valid point.
			// Drop it and log.
			s.log.Info(
				"data point being processed older than last recorded reset, will not be emitted",
				zap.String("lastRecordedReset", start.Timestamp().String()),
				zap.String("dataPoint", point.Timestamp().String()),
			)
			return nil
		}
		normalizedPoint = SubtractSummaryDataPoint(&point, start)
	}
	if (!ok && point.StartTimestamp() == 0) || !point.StartTimestamp().AsTime().Before(point.Timestamp().AsTime()) {
		// This is the first time we've seen this metric, or we received
		// an explicit reset point as described in
		// https://github.com/open-telemetry/opentelemetry-specification/blob/9555f9594c7ffe5dc333b53da5e0f880026cead1/specification/metrics/datamodel.md#resets-and-gaps
		// Record it in history and drop the point.
		s.cache.SetSummaryDataPoint(identifier, &point)
		return nil
	}
	return normalizedPoint
}

// SubtractSummaryDataPoint subtracts b from a.
func SubtractSummaryDataPoint(a, b *pmetric.SummaryDataPoint) *pmetric.SummaryDataPoint {
	// Make a copy so we don't mutate underlying data.
	newPoint := pmetric.NewSummaryDataPoint()
	// Quantile values are copied, and are not modified. Quantiles are
	// computed over the same time period as sum and count, but it isn't
	// possible to normalize them.
	a.CopyTo(newPoint)
	// Use the start timestamp from the normalization point
	newPoint.SetStartTimestamp(b.Timestamp())
	// Adjust the value based on the start point's value
	newPoint.SetCount(a.Count() - b.Count())
	// We drop points without a sum, so no need to check here.
	newPoint.SetSum(a.Sum() - b.Sum())
	return &newPoint
}
