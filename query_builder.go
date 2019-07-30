package golastic

import "errors"

type queryBuilder struct {
	wheres      []*where
	matches     []*match
	matchIns    []*matchIn
	matchNotIns []*matchNotIn
	filters     []*filter
	filterIns   []*filterIn
	whereIns    []*whereIn
	whereNotIns []*whereNotIn
	sorts       []*sort
	limit       *limit
	groupBy     *groupBy
	from        *from
}

func (qb *queryBuilder) Where(field string, operand string, value interface{}) *queryBuilder {
	qb.wheres = append(qb.wheres, &where{Field: field, Operand: operand, Value: value})

	return qb
}

func (qb *queryBuilder) WhereIn(field string, values []interface{}) *queryBuilder {
	qb.whereIns = append(qb.whereIns, &whereIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) WhereNotIn(field string, values []interface{}) *queryBuilder {
	qb.whereNotIns = append(qb.whereNotIns, &whereNotIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) Filter(field string, operand string, value interface{}) *queryBuilder {
	qb.filters = append(qb.filters, &filter{Field: field, Operand: operand, Value: value})

	return qb
}

func (qb *queryBuilder) FilterIn(field string, values []interface{}) *queryBuilder {
	qb.filterIns = append(qb.filterIns, &filterIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) Match(field string, operand string, value interface{}) *queryBuilder {
	qb.matches = append(qb.matches, &match{Field: field, Operand: operand, Value: value})

	return qb
}

func (qb *queryBuilder) MatchIn(field string, values []interface{}) *queryBuilder {
	qb.matchIns = append(qb.matchIns, &matchIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) MatchNotIn(field string, values []interface{}) *queryBuilder {
	qb.matchNotIns = append(qb.matchNotIns, &matchNotIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) OrderBy(field string, asc bool) *queryBuilder {
	qb.sorts = append(qb.sorts, &sort{Field: field, Order: asc})

	return qb
}

func (qb *queryBuilder) Limit(value int) *queryBuilder {
	qb.limit = &limit{Limit: value}

	return qb
}

func (qb *queryBuilder) GroupBy(fields ...string) *queryBuilder {
	qb.groupBy = &groupBy{Fields: fields}

	return qb
}

func (qb *queryBuilder) From(value int) *queryBuilder {
	qb.from = &from{From: value}

	return qb
}

func (qb *queryBuilder) validateWheres() error {
	for _, where := range qb.wheres {
		if err := where.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateFilters() error {
	for _, filter := range qb.filters {
		if err := filter.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateFilterIns() error {
	for _, filterIn := range qb.filterIns {
		if err := filterIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateMatches() error {
	for _, match := range qb.matches {
		if err := match.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateMatchIns() error {
	for _, matchIn := range qb.matchIns {
		if err := matchIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateMatchNotIns() error {
	for _, matchNotIn := range qb.matchNotIns {
		if err := matchNotIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateLimit() error {
	if qb.limit.Limit <= 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (qb *queryBuilder) validateFrom() error {
	if qb.from.From < 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (qb *queryBuilder) validateWhereIns() error {
	for _, whereIn := range qb.whereIns {
		if err := whereIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateWhereNotIns() error {
	for _, whereNotIn := range qb.whereNotIns {
		if err := whereNotIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateWhereClauses() error {
	if err := qb.validateWhereIns(); err != nil {
		return err
	}

	if err := qb.validateWhereNotIns(); err != nil {
		return err
	}

	return qb.validateWheres()
}

func (qb *queryBuilder) validateFilterClauses() error {
	if err := qb.validateFilterIns(); err != nil {
		return err
	}

	return qb.validateFilters()
}

func (qb *queryBuilder) validateMatchClauses() error {
	if err := qb.validateMatchIns(); err != nil {
		return err
	}

	if err := qb.validateMatchNotIns(); err != nil {
		return err
	}

	return qb.validateMatches()
}

func (qb *queryBuilder) validateMustClauses() error {
	if err := qb.validateWhereClauses(); err != nil {
		return err
	}

	if err := qb.validateFilterClauses(); err != nil {
		return err
	}

	return qb.validateMatchClauses()
}
