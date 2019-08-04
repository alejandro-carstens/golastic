package golastic

type AggregationResponses map[string]*AggregationResponse

type AggregationBuckets []*AggregationBucket

type MinMaxResponse struct {
	Min interface{} `json:"min"`
	Max interface{} `json:"max"`
}

type AggregationResponse struct {
	DocCountErrorUpperBound int                  `json:"doc_count_error_upper_bound"`
	SumOtherDocCount        int                  `json:"sum_other_doc_count"`
	Buckets                 []*AggregationBucket `json:"buckets"`
}

type AggregationBucket struct {
	Key      interface{}                     `json:"key"`
	DocCount int                             `json:"doc_count"`
	Items    map[string]*AggregationResponse `json:"items"`
}
