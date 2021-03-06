package golastic

import "errors"

type where struct {
	Field   string
	Operand string
	Value   interface{}
}

func (w *where) validate() error {
	if !inSlice(w.Operand, "<>", "=", ">", "<", "<=", ">=") {
		return errors.New("The operand is invalid.")
	}

	if !isNumeric(w.Value) && !isDate(w.Value) {
		if !isString(w.Value) {
			return errors.New("The value is not numeric, a string or a date.")
		}

		if w.Operand != "=" && w.Operand != "<>" {
			return errors.New("Value (" + w.Value.(string) + ") and operand (" + w.Operand + ") are incompatible")
		}
	}

	return nil
}

func (w *where) isString() bool {
	return isString(w.Value)
}

func (w *where) isDate() bool {
	return isDate(w.Value)
}

type whereIn struct {
	Field  string
	Values []interface{}
}

func (wi *whereIn) validate() error {
	for _, value := range wi.Values {
		if !isNumeric(value) && !isString(value) {
			return errors.New("The value is not numeric nor a string.")
		}
	}

	return nil
}

type whereNotIn struct {
	whereIn
	Field  string
	Values []interface{}
}

type filter struct {
	where
	Field   string
	Operand string
	Value   interface{}
}

func (f *filter) validate() error {
	if !inSlice(f.Operand, "=", ">", "<", "<=", ">=") {
		return errors.New("The operand is invalid.")
	}

	if !isNumeric(f.Value) && !isDate(f.Value) {
		if !isString(f.Value) {
			return errors.New("The value is not numeric, a string or a date.")
		}

		if f.Operand != "=" {
			return errors.New("Value (" + f.Value.(string) + ") and operand (" + f.Operand + ") are incompatible")
		}
	}

	return nil
}

type filterIn struct {
	whereIn
	Field  string
	Values []interface{}
}

type match struct {
	where
	Field   string
	Operand string
	Value   interface{}
}

func (m *match) validate() error {
	if !inSlice(m.Operand, "=", "<>") {
		return errors.New("The operand is invalid.")
	}

	if !isNumeric(m.Value) {
		if !isString(m.Value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if m.Operand != "=" && m.Operand != "<>" {
			return errors.New("Value (" + m.Value.(string) + ") and operand (" + m.Operand + ") are incompatible")
		}
	}

	return nil
}

type matchIn struct {
	whereIn
	Field  string
	Values []interface{}
}

type matchNotIn struct {
	whereIn
	Field  string
	Values []interface{}
}

type matchPhrase struct {
	where
	Field   string
	Operand string
	Value   interface{}
}

func (mp *matchPhrase) validate() error {
	if !inSlice(mp.Operand, "=", "<>") {
		return errors.New("The operand is invalid.")
	}

	if !isNumeric(mp.Value) {
		if !isString(mp.Value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if mp.Operand != "=" && mp.Operand != "<>" {
			return errors.New("Value (" + mp.Value.(string) + ") and operand (" + mp.Operand + ") are incompatible")
		}
	}

	return nil
}

type matchPhraseIn struct {
	matchIn
	Field  string
	Values []interface{}
}

type matchPhraseNotIn struct {
	matchNotIn
	Field  string
	Values []interface{}
}

type sort struct {
	Field string
	Order bool
}

type limit struct {
	Limit int
}

type from struct {
	From int
}

type groupBy struct {
	Fields []string
}

type nestedSort struct {
	Field string
	Path  string
	Order bool
}

type stats struct {
	Fields []string
}

func (ns *nestedSort) validate() error {
	if len(ns.Field) == 0 {
		return errors.New("field cannot be empty")
	}

	if len(ns.Path) == 0 {
		return errors.New("path cannot be empty")
	}

	return nil
}
