package daggre

type PipelineStageParams map[string]interface{}

type PipelineStage struct {
	Name   string              `json:"name"`
	Desc   string              `json:"desc"`
	Params PipelineStageParams `json:"params"`
}

type Pipeline struct {
	Name   string           `json:"name"`
	Desc   string           `json:"desc"`
	Tables []string         `json:"tables"`
	Stages []*PipelineStage `json:"stages"`
}
