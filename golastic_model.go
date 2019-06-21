package golastic

type GolasticModel struct {
	ElasticModel
}

const GOLASTIC_MODEL string = "golastic.GolasticModel"

func NewGolasticModel() *GolasticModel {
	model := new(GolasticModel)

	model.isGolasticModel = true

	return model
}

func (gm *GolasticModel) SetPropertiesMap(propertiesMap map[string]interface{}) {
	gm.propertiesMap = propertiesMap
}

func (gm *GolasticModel) SetProperties(properties []string) {
	gm.properties = properties
}

func (gm *GolasticModel) SetIndex(index string) {
	gm.index = index
}

func (gm *GolasticModel) SetMappings(mappings map[string]interface{}) {
	gm.mappings = mappings
}

func (gm *GolasticModel) Properties() []string {
	return gm.properties
}

func (gm *GolasticModel) Index() string {
	return gm.index
}

func (gm *GolasticModel) GetId() string {
	return gm.Id
}

func (gm *GolasticModel) PropertiesMap() map[string]interface{} {
	return gm.propertiesMap
}

func (gm *GolasticModel) SetId(id string) {
	gm.Id = id
}

func (gm *GolasticModel) SetData(data map[string]interface{}) {
	if _, ok := data["Id"]; ok {
		gm.SetId(data["Id"].(string))
	} else if _, ok := data["id"]; ok {
		gm.SetId(data["id"].(string))
	}

	gm.data = data
}
