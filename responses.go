package golastic

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

type AggregationResponses map[string]*AggregationResponse

type AggregationBuckets []*AggregationBucket

type MinMaxResponse struct {
	Min interface{} `json:"min"`
	Max interface{} `json:"max"`
}

type CatAliasesResponse struct {
	Alias         string `json:"alias"`
	Index         string `json:"index"`
	Filter        string `json:"filter"`
	RoutingIndex  string `json:"routing.index"`
	RoutingSearch string `json:"routing.search"`
}
