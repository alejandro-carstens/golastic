package golastic

import "testing"

func TestWheres(t *testing.T) {
	builder := new(queryBuilder)
	builder.Where("description", "=", "value1").
		Where("description", "<>", "value2").
		Where("subject_id", ">", 1).
		Where("subject_id", "<", 5).
		Where("subject_id", ">=", 1).
		Where("subject_id", "<=", 4)

	if got := builder.validateWhereClauses(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Where("subject_id", "!=", 0)

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Where("subject_id", ">", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Where("subject_id", "<", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Where("subject_id", ">=", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Where("subject_id", "<=", "value1")

	if got := builder.validateWhereClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestFilters(t *testing.T) {
	builder := new(queryBuilder)
	builder.Filter("description", "=", "value1").
		Filter("subject_id", ">", 1).
		Filter("subject_id", "<", 5).
		Filter("subject_id", ">=", 1).
		Filter("subject_id", "<=", 4)

	if got := builder.validateFilters(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Filter("subject_id", "<>", 0)

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Filter("subject_id", ">", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Filter("subject_id", "<", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Filter("subject_id", ">=", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Filter("subject_id", "<=", "value1")

	if got := builder.validateFilters(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestMatches(t *testing.T) {
	builder := new(queryBuilder)
	builder.Match("description", "=", "value1").
		Match("description", "<>", "value2")

	if got := builder.validateMatchClauses(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Match("subject_id", "!=", 0)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Match("subject_id", "<", 1)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Match("subject_id", ">", 1)

	if got := builder.validateMatchClauses(); got == nil {
		t.Error("Expected errors but got ", got)
	}
}

func TestWhereIn(t *testing.T) {
	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.WhereIn("description", descriptions).
		WhereIn("subject_id", subjectIds)

	if got := builder.validateWhereIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.WhereIn("Fakedescription", descriptions)

	if got := builder.validateWhereIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestFilterIn(t *testing.T) {
	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.FilterIn("description", descriptions).
		FilterIn("subject_id", subjectIds)

	if got := builder.validateFilterIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.FilterIn("Fakedescription", descriptions)

	if got := builder.validateFilterIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestMatchIn(t *testing.T) {
	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.MatchIn("description", descriptions).
		MatchIn("subject_id", subjectIds)

	if got := builder.validateMatchIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.MatchIn("Fakedescription", descriptions)

	if got := builder.validateMatchIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestWhereNotIn(t *testing.T) {
	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.WhereNotIn("description", descriptions).
		WhereNotIn("subject_id", subjectIds)

	if got := builder.validateWhereNotIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.WhereNotIn("Fakedescription", descriptions)

	if got := builder.validateWhereNotIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestMatchNotIn(t *testing.T) {
	var descriptions = []interface{}{"value1", "value2", "value3"}
	var subjectIds = []interface{}{1, 2, 4}

	builder := new(queryBuilder)
	builder.MatchNotIn("description", descriptions).
		MatchNotIn("subject_id", subjectIds)

	if got := builder.validateMatchNotIns(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.MatchNotIn("Fakedescription", descriptions)

	if got := builder.validateMatchNotIns(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestLimit(t *testing.T) {
	builder := new(queryBuilder)
	builder.Limit(10)

	if got := builder.validateLimit(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.Limit(-10)

	if got := builder.validateLimit(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}

func TestFrom(t *testing.T) {
	builder := new(queryBuilder)
	builder.From(10)

	if got := builder.validateFrom(); got != nil {
		t.Error("Expected no errors but got ", got)
	}

	builder = new(queryBuilder)
	builder.From(-10)

	if got := builder.validateFrom(); got == nil {
		t.Error("Expected no errors but got ", got)
	}
}
