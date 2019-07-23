package golastic

import "testing"

func TestWheres(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.Where("Description", "=", "value1").
		Where("Description", "<>", "value2").
		Where("SubjectId", ">", 1).
		Where("SubjectId", "<", 5).
		Where("SubjectId", ">=", 1).
		Where("SubjectId", "<=", 4)

	if got := builder.validateWhereClauses(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Where("SubjectId", "!=", 0)

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Where("SubjectId", ">", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Where("SubjectId", "<", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Where("SubjectId", ">=", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Where("SubjectId", "<=", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestFilters(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("Description", "=", "value1").
		Filter("SubjectId", ">", 1).
		Filter("SubjectId", "<", 5).
		Filter("SubjectId", ">=", 1).
		Filter("SubjectId", "<=", 4)

	if got := builder.validateFilters(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("SubjectId", "<>", 0)

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("SubjectId", ">", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("SubjectId", "<", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("SubjectId", ">=", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Filter("SubjectId", "<=", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestMatches(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.Match("Description", "=", "value1").
		Match("Description", "<>", "value2")

	if got := builder.validateMatchClauses(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Match("SubjectId", "!=", 0)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Match("SubjectId", "<", 1)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Match("SubjectId", ">", 1)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestWhereIn(t *testing.T) {
	example := new(Example).New()

	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.WhereIn("Description", descriptions).
		WhereIn("SubjectId", subjectIds)

	if got := builder.validateWhereIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.WhereIn("FakeDescription", descriptions)

	if got := builder.validateWhereIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestFilterIn(t *testing.T) {
	example := new(Example).New()

	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.FilterIn("Description", descriptions).
		FilterIn("SubjectId", subjectIds)

	if got := builder.validateFilterIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.FilterIn("FakeDescription", descriptions)

	if got := builder.validateFilterIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestMatchIn(t *testing.T) {
	example := new(Example).New()

	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.MatchIn("Description", descriptions).
		MatchIn("SubjectId", subjectIds)

	if got := builder.validateMatchIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.MatchIn("FakeDescription", descriptions)

	if got := builder.validateMatchIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestWhereNotIn(t *testing.T) {
	example := new(Example).New()

	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.WhereNotIn("Description", descriptions).
		WhereNotIn("SubjectId", subjectIds)

	if got := builder.validateWhereNotIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.WhereNotIn("FakeDescription", descriptions)

	if got := builder.validateWhereNotIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestMatchNotIn(t *testing.T) {
	example := new(Example).New()

	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.MatchNotIn("Description", descriptions).
		MatchNotIn("SubjectId", subjectIds)

	if got := builder.validateMatchNotIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.MatchNotIn("FakeDescription", descriptions)

	if got := builder.validateMatchNotIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestLimit(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.Limit(10)

	if got := builder.validateLimit(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.Limit(-10)

	if got := builder.validateLimit(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestGroupBy(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.GroupBy("Description")

	if got := builder.validateGroupBy(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.GroupBy("FakeDescription")

	if got := builder.validateGroupBy(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestOrderBy(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.OrderBy("SubjectId", true)

	if got := builder.validateOrders(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.OrderBy("FakeSubjectId", true)

	if got := builder.validateOrders(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestGroupByMany(t *testing.T) {
	example := new(Example).New()

	var fields = []string{
		"Description",
		"SubjectId",
	}

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.GroupBy(fields...)

	if got := builder.validateGroupBy(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	fields = []string{
		"FakeDescription",
		"FakeSubjectId",
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.GroupBy(fields...)

	if got := builder.validateGroupBy(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestFrom(t *testing.T) {
	example := new(Example).New()

	builder := new(queryBuilder)
	builder.SetModel(example)
	builder.From(10)

	if got := builder.validateFrom(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.SetModel(example)
	builder.From(-10)

	if got := builder.validateFrom(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}
