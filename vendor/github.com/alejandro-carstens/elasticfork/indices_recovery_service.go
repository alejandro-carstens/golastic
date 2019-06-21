package elasticfork

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/alejandro-carstens/elasticfork/uritemplates"
)

type IndicesRecoveryService struct {
	client     *Client
	indices    []string
	human      *bool
	detailed   *bool
	activeOnly *bool
}

func NewIndicesRecoveryService(client *Client) *IndicesRecoveryService {
	return &IndicesRecoveryService{
		client: client,
	}
}

func (irs *IndicesRecoveryService) Indices(indices ...string) *IndicesRecoveryService {
	irs.indices = indices
	return irs
}

func (irs *IndicesRecoveryService) Human(human bool) *IndicesRecoveryService {
	irs.human = &human
	return irs
}

func (irs *IndicesRecoveryService) buildURL() (string, url.Values, error) {
	var err error
	var path string

	if len(irs.indices) > 0 {
		path, err = uritemplates.Expand("/{indices}/_recovery", map[string]string{
			"indices": strings.Join(irs.indices, ","),
		})
	} else {
		path, err = uritemplates.Expand("/_recovery", map[string]string{})
	}

	if err != nil {
		return "", url.Values{}, err
	}

	params := url.Values{}

	if irs.human != nil {
		params.Set("human", fmt.Sprintf("%v", *irs.human))
	}

	if irs.detailed != nil {
		params.Set("detailed", fmt.Sprintf("%v", *irs.detailed))
	}

	if irs.activeOnly != nil {
		params.Set("detailed", fmt.Sprintf("%v", *irs.activeOnly))
	}

	return path, params, nil
}

func (irs *IndicesRecoveryService) Do(ctx context.Context) (map[string]*IndexRecoveryResponse, error) {
	path, params, err := irs.buildURL()

	if err != nil {
		return nil, err
	}

	res, err := irs.client.PerformRequest(ctx, PerformRequestOptions{
		Method: "GET",
		Path:   path,
		Params: params,
	})

	if err != nil {
		return nil, err
	}

	rawRecoveryResponse := map[string]interface{}{}

	if err := json.Unmarshal(res.Body, &rawRecoveryResponse); err != nil {
		return nil, err
	}

	indicesRecoveryResponse := map[string]*IndexRecoveryResponse{}

	for snapshot, response := range rawRecoveryResponse {
		b, err := json.Marshal(response)

		if err != nil {
			return nil, err
		}

		indexRecoveryResponse := new(IndexRecoveryResponse)

		if err := json.Unmarshal(b, indexRecoveryResponse); err != nil {
			return nil, err
		}

		indicesRecoveryResponse[snapshot] = indexRecoveryResponse
	}

	return indicesRecoveryResponse, nil
}

type IndexRecoveryResponse struct {
	Shards []*IndexRecoveryShardResponse `json:"shards"`
}

type IndexRecoveryShardResponse struct {
	Id                int64                     `json:"id"`
	Primary           bool                      `json:"primary"`
	Stage             string                    `json:"stage"`
	StartTime         string                    `json:"start_time"`
	StartTimeInMillis int64                     `json:"start_time_in_millis"`
	StopTime          string                    `json:"stop_time"`
	StopTimeInMillis  int64                     `json:"stop_time_in_millis"`
	TotalTime         string                    `json:"total_time"`
	TotalTimeInMillis int64                     `json:"total_time_in_millis"`
	Type              string                    `json:"type"`
	Source            *IndexRecoverySource      `json:"source"`
	Target            *IndexRecoveryTarget      `json:"target"`
	Translog          *IndexRecoveryTranslog    `json:"translog"`
	Index             *RecoveryIndex            `json:"index"`
	VerifyIndex       *IndexRecoveryVerifyIndex `json:"verify_index"`
}

type RecoveryIndex struct {
	Files                      *RecoveryIndexFiles `json:"files"`
	Size                       *RecoveryIndexSize  `json:"size"`
	SourceThrottleTime         string              `json:"source_throttle_time"`
	SourceThrottleTimeInMillis int64               `json:"source_throttle_time_in_millis"`
	TargetThrottleTime         string              `json:"target_throttle_time"`
	TargetThrottleTimeInMillis int64               `json:"target_throttle_time_in_millis"`
	TotalTime                  string              `json:"total_time"`
	TotalTimeInMillis          int64               `json:"total_time_in_millis"`
}

type RecoveryIndexFiles struct {
	Recovered int64  `json:"recovered"`
	Percent   string `json:"percent"`
	Reused    int64  `json:"reused"`
	Totla     int64  `json:"total"`
}

type RecoveryIndexSize struct {
	Percent          string `json:"percent"`
	Recovered        string `json:"recovered"`
	RecoveredInBytes int64  `json:"recovered_in_bytes"`
	Reused           string `json:"reused"`
	ReusedInBytes    int64  `json:"reused_in_bytes"`
	Total            string `json:"total"`
	TotalInBytes     int64  `json:"total_in_bytes"`
}

type IndexRecoverySource struct {
	Index       string `json:"index"`
	Repository  string `json"repository"`
	RestoreUUID string `json:"restoreUUID"`
	Snapshot    string `json:"snapshot"`
	Version     string `json"version"`
}

type IndexRecoveryTarget struct {
	Host             string `json:"host"`
	Id               string `json:"id"`
	Ip               string `json:"ip"`
	Name             string `json:"name"`
	TransportAddress string `json:"transport_address"`
}

type IndexRecoveryTranslog struct {
	Percent         string `json:"percent"`
	Recovered       int64  `json:"recovered"`
	Total           int64  `json:"total"`
	TotalOnStart    int64  `json:"total_on_start"`
	TotalTime       string `json:"total_time"`
	TotalTimeMillis int64  `json:"total_time_in_millis"`
}

type IndexRecoveryVerifyIndex struct {
	CheckIndexTime         string `json:"check_index_time"`
	CheckIndexTimeInMillis int64  `json:"check_index_time_in_millis"`
	TotalTime              string `json:"total_time"`
	TotalTimeInMillis      int64  `json:"total_time_in_millis"`
}
