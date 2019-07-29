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

func (b *queryBuilder) Where(field string, operand string, value interface{}) *queryBuilder {
	b.wheres = append(b.wheres, &where{Field: field, Operand: operand, Value: value})

	return b
}

func (b *queryBuilder) WhereIn(field string, values []interface{}) *queryBuilder {
	b.whereIns = append(b.whereIns, &whereIn{Field: field, Values: values})

	return b
}

func (b *queryBuilder) WhereNotIn(field string, values []interface{}) *queryBuilder {
	b.whereNotIns = append(b.whereNotIns, &whereNotIn{Field: field, Values: values})

	return b
}

func (b *queryBuilder) Filter(field string, operand string, value interface{}) *queryBuilder {
	b.filters = append(b.filters, &filter{Field: field, Operand: operand, Value: value})

	return b
}

func (b *queryBuilder) FilterIn(field string, values []interface{}) *queryBuilder {
	b.filterIns = append(b.filterIns, &filterIn{Field: field, Values: values})

	return b
}

func (b *queryBuilder) Match(field string, operand string, value interface{}) *queryBuilder {
	b.matches = append(b.matches, &match{Field: field, Operand: operand, Value: value})

	return b
}

func (b *queryBuilder) MatchIn(field string, values []interface{}) *queryBuilder {
	b.matchIns = append(b.matchIns, &matchIn{Field: field, Values: values})

	return b
}

func (b *queryBuilder) MatchNotIn(field string, values []interface{}) *queryBuilder {
	b.matchNotIns = append(b.matchNotIns, &matchNotIn{Field: field, Values: values})

	return b
}

func (b *queryBuilder) OrderBy(field string, asc bool) *queryBuilder {
	b.sorts = append(b.sorts, &sort{Field: field, Order: asc})

	return b
}

func (b *queryBuilder) Limit(value int) *queryBuilder {
	b.limit = &limit{Limit: value}

	return b
}

func (b *queryBuilder) GroupBy(fields ...string) *queryBuilder {
	b.groupBy = &groupBy{Fields: fields}

	return b
}

func (b *queryBuilder) From(value int) *queryBuilder {
	b.from = &from{From: value}

	return b
}

func (b *queryBuilder) validateWheres() error {
	for _, where := range b.wheres {
		if err := where.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateFilters() error {
	for _, filter := range b.filters {
		if err := filter.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateFilterIns() error {
	for _, filterIn := range b.filterIns {
		if err := filterIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatches() error {
	for _, match := range b.matches {
		if err := match.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatchIns() error {
	for _, matchIn := range b.matchIns {
		if err := matchIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatchNotIns() error {
	for _, matchNotIn := range b.matchNotIns {
		if err := matchNotIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateLimit() error {
	if b.limit.Limit <= 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (b *queryBuilder) validateFrom() error {
	if b.from.From < 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (b *queryBuilder) validateWhereIns() error {
	for _, whereIn := range b.whereIns {
		if err := whereIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateWhereNotIns() error {
	for _, whereNotIn := range b.whereNotIns {
		if err := whereNotIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateWhereClauses() error {
	if err := b.validateWhereIns(); err != nil {
		return err
	}

	if err := b.validateWhereNotIns(); err != nil {
		return err
	}

	return b.validateWheres()
}

func (b *queryBuilder) validateFilterClauses() error {
	if err := b.validateFilterIns(); err != nil {
		return err
	}

	return b.validateFilters()
}

func (b *queryBuilder) validateMatchClauses() error {
	if err := b.validateMatchIns(); err != nil {
		return err
	}

	if err := b.validateMatchNotIns(); err != nil {
		return err
	}

	return b.validateMatches()
}

func (b *queryBuilder) validateMustClauses() error {
	if err := b.validateWhereClauses(); err != nil {
		return err
	}

	if err := b.validateFilterClauses(); err != nil {
		return err
	}

	return b.validateMatchClauses()
}
