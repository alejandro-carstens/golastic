package golastic

type Shard struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
}

type BaseWriteResponse struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	Id          string `json:"_id"`
	Version     int    `json:"_version"`
	Result      string `json:"result"`
	Shards      Shard  `json:"_shards"`
	SeqNo       int    `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
	Status      int    `json:"status"`
}

type WriteResponse struct {
	Took  int `json:"took"`
	Items []struct {
		Create *BaseWriteResponse `json:"create"`
		Update *BaseWriteResponse `json:"update"`
		Delete *BaseWriteResponse `json:"delete"`
	} `json:"items"`
}

func (r *WriteResponse) GetItems() []*BaseWriteResponse {
	items := []*BaseWriteResponse{}

	for _, item := range r.Items {
		if item.Create != nil {
			items = append(items, item.Create)
		}
		if item.Update != nil {
			items = append(items, item.Update)
		}
		if item.Delete != nil {
			items = append(items, item.Delete)
		}
	}

	return items
}

func (r *WriteResponse) First() *BaseWriteResponse {
	var response *BaseWriteResponse

	for _, item := range r.GetItems() {
		response = item

		break
	}

	return response
}

type WriteByQueryResponse struct {
	Took             int  `json:"took"`
	TimedOut         bool `json:"timed_out"`
	Total            int  `json:"total"`
	Updated          int  `json:"updated"`
	Deleted          int  `json:"deleted"`
	Batches          int  `json:"batches"`
	VersionConflicts int  `json:"version_conflicts"`
	Noops            int  `json:"noops"`
	Retries          struct {
		Bulk   int `json:"bulk"`
		Search int `json:"search"`
	} `json:"retries"`
	Throttled            string        `json:"throttled"`
	ThrottledMillis      int           `json:"throttled_millis"`
	RequestsPerSecond    int           `json:"requests_per_second"`
	ThrottledUntil       string        `json:"throttled_until"`
	ThrottledUntilMillis int           `json:"throttled_until_millis"`
	Failures             []interface{} `json:"failures"`
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
