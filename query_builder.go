package golastic

import (
	"errors"
	"strings"
)

type nested struct {
	wheres       []*where
	whereIns     []*whereIn
	whereNotIns  []*whereNotIn
	filters      []*filter
	filterIns    []*filterIn
	matches      []*match
	matchPhrases []*matchPhrase
}

type queryBuilder struct {
	wheres            []*where
	matches           []*match
	matchIns          []*matchIn
	matchNotIns       []*matchNotIn
	matchPhrases      []*matchPhrase
	matchPhraseIns    []*matchPhraseIn
	matchPhraseNotIns []*matchPhraseNotIn
	filters           []*filter
	filterIns         []*filterIn
	whereIns          []*whereIn
	whereNotIns       []*whereNotIn
	sorts             []*sort
	limit             *limit
	groupBy           *groupBy
	from              *from
	nested            map[string]*nested
	nestedSort        *nestedSort
	stats             *stats
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

func (qb *queryBuilder) MatchPhrase(field string, operand string, value interface{}) *queryBuilder {
	qb.matchPhrases = append(qb.matchPhrases, &matchPhrase{Field: field, Operand: operand, Value: value})

	return qb
}

func (qb *queryBuilder) MatchPhraseIn(field string, values []interface{}) *queryBuilder {
	qb.matchPhraseIns = append(qb.matchPhraseIns, &matchPhraseIn{Field: field, Values: values})

	return qb
}

func (qb *queryBuilder) MatchPhraseNotIn(field string, values []interface{}) *queryBuilder {
	qb.matchPhraseNotIns = append(qb.matchPhraseNotIns, &matchPhraseNotIn{Field: field, Values: values})

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

func (qb *queryBuilder) Stats(fields ...string) *queryBuilder {
	qb.stats = &stats{Fields: fields}

	return qb
}

func (qb *queryBuilder) From(value int) *queryBuilder {
	qb.from = &from{From: value}

	return qb
}

func (qb *queryBuilder) WhereNested(field string, operand string, value interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].wheres) == 0 {
		qb.nested[path].wheres = []*where{}
	}

	qb.nested[path].wheres = append(qb.nested[path].wheres, &where{
		Field:   field,
		Operand: operand,
		Value:   value,
	})

	return qb
}

func (qb *queryBuilder) WhereInNested(field string, values []interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].wheres) == 0 {
		qb.nested[path].whereIns = []*whereIn{}
	}

	qb.nested[path].whereIns = append(qb.nested[path].whereIns, &whereIn{
		Field:  field,
		Values: values,
	})

	return qb
}

func (qb *queryBuilder) WhereNotInNested(field string, values []interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].wheres) == 0 {
		qb.nested[path].whereNotIns = []*whereNotIn{}
	}

	qb.nested[path].whereNotIns = append(qb.nested[path].whereNotIns, &whereNotIn{
		Field:  field,
		Values: values,
	})

	return qb
}

func (qb *queryBuilder) FilterNested(field string, operand string, value interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].filters) == 0 {
		qb.nested[path].filters = []*filter{}
	}

	qb.nested[path].filters = append(qb.nested[path].filters, &filter{
		Field:   field,
		Operand: operand,
		Value:   value,
	})

	return qb
}

func (qb *queryBuilder) FilterInNested(field string, values []interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].filterIns) == 0 {
		qb.nested[path].filterIns = []*filterIn{}
	}

	qb.nested[path].filterIns = append(qb.nested[path].filterIns, &filterIn{
		Field:  field,
		Values: values,
	})

	return qb
}

func (qb *queryBuilder) MatchNested(field string, operand string, value interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].matches) == 0 {
		qb.nested[path].matches = []*match{}
	}

	qb.nested[path].matches = append(qb.nested[path].matches, &match{
		Field:   field,
		Operand: operand,
		Value:   value,
	})

	return qb
}

func (qb *queryBuilder) MatchInNested(field string, values []interface{}) *queryBuilder {
	for _, value := range values {
		qb.MatchNested(field, "=", value)
	}

	return qb
}

func (qb *queryBuilder) MatchNotInNested(field string, values []interface{}) *queryBuilder {
	for _, value := range values {
		qb.MatchNested(field, "<>", value)
	}

	return qb
}

func (qb *queryBuilder) MatchPhraseNested(field string, operand string, value interface{}) *queryBuilder {
	if len(qb.nested) == 0 {
		qb.nested = map[string]*nested{}
	}

	path := strings.Split(field, ".")[0]

	if _, valid := qb.nested[path]; !valid {
		qb.nested[path] = &nested{}
	}

	if len(qb.nested[path].matchPhrases) == 0 {
		qb.nested[path].matchPhrases = []*matchPhrase{}
	}

	qb.nested[path].matchPhrases = append(qb.nested[path].matchPhrases, &matchPhrase{
		Field:   field,
		Operand: operand,
		Value:   value,
	})

	return qb
}

func (qb *queryBuilder) MatchPhraseInNested(field string, values []interface{}) *queryBuilder {
	for _, value := range values {
		qb.MatchPhraseNested(field, "=", value)
	}

	return qb
}

func (qb *queryBuilder) MatchPhraseNotInNested(field string, values []interface{}) *queryBuilder {
	for _, value := range values {
		qb.MatchPhraseNested(field, "<>", value)
	}

	return qb
}

func (qb *queryBuilder) OrderByNested(path string, order bool) *queryBuilder {
	qb.nestedSort = &nestedSort{Order: order, Field: path}

	pieces := strings.Split(path, ".")

	if len(pieces) > 1 {
		qb.nestedSort.Path = pieces[0]
	}

	return qb
}

func (qb *queryBuilder) Clear() *queryBuilder {
	qb.wheres = nil
	qb.matches = nil
	qb.matchIns = nil
	qb.matchNotIns = nil
	qb.filters = nil
	qb.filterIns = nil
	qb.whereIns = nil
	qb.whereNotIns = nil
	qb.sorts = nil
	qb.limit = nil
	qb.groupBy = nil
	qb.from = nil
	qb.nested = nil
	qb.nestedSort = nil

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

func (qb *queryBuilder) validateNestedClauses() error {
	for path, nested := range qb.nested {
		if len(path) == 0 {
			return errors.New("Path cannot be empty")
		}

		for _, where := range nested.wheres {
			if err := where.validate(); err != nil {
				return err
			}

			if len(strings.Split(where.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, whereIn := range nested.whereIns {
			if err := whereIn.validate(); err != nil {
				return err
			}

			if len(strings.Split(whereIn.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, whereNotIn := range nested.whereNotIns {
			if err := whereNotIn.validate(); err != nil {
				return err
			}

			if len(strings.Split(whereNotIn.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, filter := range nested.filters {
			if err := filter.validate(); err != nil {
				return err
			}

			if len(strings.Split(filter.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, filterIn := range nested.filterIns {
			if err := filterIn.validate(); err != nil {
				return err
			}

			if len(strings.Split(filterIn.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, match := range nested.matches {
			if err := match.validate(); err != nil {
				return err
			}

			if len(strings.Split(match.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
		}

		for _, matchPhrase := range nested.matchPhrases {
			if err := matchPhrase.validate(); err != nil {
				return err
			}

			if len(strings.Split(matchPhrase.Field, ".")) < 2 {
				return errors.New("Wrong nested notation, needs to be 'object.property'")
			}
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

func (qb *queryBuilder) validateMatchPhrases() error {
	for _, matchPhrase := range qb.matchPhrases {
		if err := matchPhrase.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateMatchPhraseIns() error {
	for _, matchPhraseIn := range qb.matchPhraseIns {
		if err := matchPhraseIn.validate(); err != nil {
			return err
		}
	}

	return nil
}

func (qb *queryBuilder) validateMatchPhraseNotIns() error {
	for _, matchPhraseNotIn := range qb.matchPhraseNotIns {
		if err := matchPhraseNotIn.validate(); err != nil {
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

func (qb *queryBuilder) validateMatchPhraseClauses() error {
	if err := qb.validateMatchPhraseIns(); err != nil {
		return err
	}

	if err := qb.validateMatchPhraseNotIns(); err != nil {
		return err
	}

	return qb.validateMatchPhrases()
}

func (qb *queryBuilder) validateMustClauses() error {
	if err := qb.validateWhereClauses(); err != nil {
		return err
	}

	if err := qb.validateFilterClauses(); err != nil {
		return err
	}

	if err := qb.validateMatchClauses(); err != nil {
		return err
	}

	if err := qb.validateMatchPhraseClauses(); err != nil {
		return err
	}

	return qb.validateNestedClauses()
}
