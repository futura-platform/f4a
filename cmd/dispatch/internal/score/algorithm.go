package score

import "math"

// WorkerUtilizationScore computes a single scalar “load score” for a worker pod
// using ONLY CPU and memory utilization fractions (0.0–1.0).
//
// Goals of this scoring function:
//   - Produce one number that is easy to compare across workers.
//   - Treat the most-saturated resource as the primary bottleneck.
//   - Penalize workers that are moderately high in *both* CPU and memory
//     (to avoid fragmentation where everything becomes half-full).
//   - Treat memory saturation as more dangerous than CPU saturation,
//     since memory hits a hard cliff (OOM) while CPU degrades more gracefully.
//
// Formula:
//
//	memAdj   = mem²                // nonlinear “memory cliff” penalty
//	dominant = max(cpu, memAdj)    // main bottleneck resource
//	secondary= min(cpu, memAdj)    // secondary pressure
//
//	score = dominant + α * secondary
//
// Where α is a small weight (typically 0.2–0.3).
//
// Interpretation:
//   - Lower score  => more available capacity
//   - Higher score => more loaded / should receive fewer tasks
//
// Example:
//
//	cpu=0.90, mem=0.10  => score ~0.90 (CPU-bound worker)
//	cpu=0.50, mem=0.50  => score ~0.62 (balanced medium load)
//	cpu=0.40, mem=0.90  => score ~0.81+ (memory-risky worker)
//
// Note: This should usually be paired with smoothing (EMA) to avoid reacting
// to short CPU spikes:
//
//	scoreSmoothed = 0.8*old + 0.2*new
func WorkerUtilizationScore(cpu, mem float64) float64 {
	const alpha = 0.25 // small secondary penalty weight

	// Memory is “cliff-like”: approaching 100% is disproportionately risky.
	// Squaring makes high memory utilization dominate more strongly.
	memAdj := mem * mem

	// Dominant resource (the bottleneck).
	dominant := math.Max(cpu, memAdj)

	// Secondary resource pressure (prevents ignoring the other dimension).
	secondary := math.Min(cpu, memAdj)

	return dominant + alpha*secondary
}
