package golastic

// AggregationResponses represents a map for *AggregationResponse
type AggregationResponses map[string]*AggregationResponse

// AggregationBuckets represents a slice of AggregationBucket
type AggregationBuckets []*AggregationBucket

// MinMaxResponse is the response for the MinMax builder call
type MinMaxResponse struct {
	Min interface{} `json:"min"`
	Max interface{} `json:"max"`
}

// AggregationResponse represents an aggregation's query response
type AggregationResponse struct {
	DocCountErrorUpperBound int                  `json:"doc_count_error_upper_bound"`
	SumOtherDocCount        int                  `json:"sum_other_doc_count"`
	Buckets                 []*AggregationBucket `json:"buckets"`
}

// AggregationBucket represents a bucket within an AggregationResponse
type AggregationBucket struct {
	Key      interface{}                     `json:"key"`
	DocCount int                             `json:"doc_count"`
	Items    map[string]*AggregationResponse `json:"items"`
}
