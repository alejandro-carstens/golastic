package golastic

import (
	"errors"
)

type queryBuilder struct {
	wheres      []*Where
	matches     []*Match
	matchIns    []*MatchIn
	matchNotIns []*MatchNotIn
	filters     []*Filter
	filterIns   []*FilterIn
	whereIns    []*WhereIn
	whereNotIns []*WhereNotIn
	sorts       []*Sort
	limit       *Limit
	groupBy     *GroupBy
	from        *From
	model       ElasticModelable
}

func (b *queryBuilder) SetModel(model ElasticModelable) (QueryBuildable, error) {
	if err := model.Validate(); err != nil {
		return nil, err
	}

	b.model = model

	return b, nil
}

func (b *queryBuilder) Where(field string, operand string, value interface{}) QueryBuildable {
	where := new(Where).New(field, operand, value)

	temp := b.wheres
	b.wheres = append(temp, where)

	return b
}

func (b *queryBuilder) WhereIn(field string, values []interface{}) QueryBuildable {
	whereIn := new(WhereIn).New(field, values)

	temp := b.whereIns
	b.whereIns = append(temp, whereIn)

	return b
}

func (b *queryBuilder) WhereNotIn(field string, values []interface{}) QueryBuildable {
	whereNotIn := new(WhereNotIn).New(field, values)

	temp := b.whereNotIns
	b.whereNotIns = append(temp, whereNotIn)

	return b
}

func (b *queryBuilder) Filter(field string, operand string, value interface{}) QueryBuildable {
	filter := new(Filter).New(field, operand, value)

	temp := b.filters
	b.filters = append(temp, filter)

	return b
}

func (b *queryBuilder) FilterIn(field string, values []interface{}) QueryBuildable {
	filterIn := new(FilterIn).New(field, values)

	temp := b.filterIns
	b.filterIns = append(temp, filterIn)

	return b
}

func (b *queryBuilder) Match(field string, operand string, value interface{}) QueryBuildable {
	match := new(Match).New(field, operand, value)

	temp := b.matches
	b.matches = append(temp, match)

	return b
}

func (b *queryBuilder) MatchIn(field string, values []interface{}) QueryBuildable {
	matchIn := new(MatchIn).New(field, values)

	temp := b.matchIns
	b.matchIns = append(temp, matchIn)

	return b
}

func (b *queryBuilder) MatchNotIn(field string, values []interface{}) QueryBuildable {
	matchNotIn := new(MatchNotIn).New(field, values)

	temp := b.matchNotIns
	b.matchNotIns = append(temp, matchNotIn)

	return b
}

func (b *queryBuilder) OrderBy(field string, asc bool) QueryBuildable {
	sort := new(Sort).New(field, asc)

	temp := b.sorts
	b.sorts = append(temp, sort)

	return b
}

func (b *queryBuilder) Limit(limit int) QueryBuildable {
	b.limit = new(Limit).New(limit)

	return b
}

func (b *queryBuilder) GroupBy(fields ...string) QueryBuildable {
	b.groupBy = new(GroupBy).New(fields)

	return b
}

func (b *queryBuilder) From(from int) QueryBuildable {
	b.from = new(From).New(from)

	return b
}

func (b *queryBuilder) validateWheres() error {
	for _, where := range b.wheres {
		if err := b.validateField(where.GetField()); err != nil {
			return err
		}

		if err := where.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateFilters() error {
	for _, filter := range b.filters {
		if err := b.validateField(filter.GetField()); err != nil {
			return err
		}

		if err := filter.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateFilterIns() error {
	for _, filterIn := range b.filterIns {
		if err := b.validateField(filterIn.GetField()); err != nil {
			return err
		}

		if err := filterIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatches() error {
	for _, match := range b.matches {
		if err := b.validateField(match.GetField()); err != nil {
			return err
		}

		if err := match.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatchIns() error {
	for _, matchIn := range b.matchIns {
		if err := b.validateField(matchIn.GetField()); err != nil {
			return err
		}

		if err := matchIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateMatchNotIns() error {
	for _, matchNotIn := range b.matchNotIns {
		if err := b.validateField(matchNotIn.GetField()); err != nil {
			return err
		}

		if err := matchNotIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateOrders() error {
	for _, sort := range b.sorts {
		if err := b.validateField(sort.GetField()); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateLimit() error {
	if b.limit.GetLimit() <= 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (b *queryBuilder) validateFrom() error {
	if b.from.GetFrom() < 0 {
		return errors.New("The limit needs to be greater than 0.")
	}

	return nil
}

func (b *queryBuilder) validateWhereIns() error {
	for _, whereIn := range b.whereIns {
		if err := b.validateField(whereIn.GetField()); err != nil {
			return err
		}

		if err := whereIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateGroupBy() error {
	for _, field := range b.groupBy.GetFields() {
		if err := b.validateField(field); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateWhereNotIns() error {
	for _, whereNotIn := range b.whereNotIns {
		if err := b.validateField(whereNotIn.GetField()); err != nil {
			return err
		}

		if err := whereNotIn.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (b *queryBuilder) validateField(field string) error {
	properties := b.model.Properties()
	check := false

	for _, property := range properties {
		if field == property {
			check = true
			break
		}
	}

	if !check {
		return errors.New("The field does not match any property.")
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
