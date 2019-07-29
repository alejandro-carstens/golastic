package golastic

import "errors"

type whereIn struct {
	Field  string
	Values []interface{}
}

func (wi *whereIn) Validate() error {
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

type where struct {
	Field   string
	Operand string
	Value   interface{}
}

func (w *where) Validate() error {
	if err := w.validateOperand(w.Operand); err != nil {
		return err
	}

	if !isNumeric(w.Value) {
		if !isString(w.Value) {
			return errors.New("The value is not numeric nor a string.")
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

func (w *where) IsDate() bool {
	return IsDate(w.Value)
}

func (w *where) validateOperand(operand string) error {
	if operand == "<>" || operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

type filter struct {
	where
	Field   string
	Operand string
	Value   interface{}
}

func (f *filter) Validate() error {
	if err := f.validateOperand(f.Operand); err != nil {
		return err
	}

	if !isNumeric(f.Value) {
		if !isString(f.Value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if f.Operand != "=" {
			return errors.New("Value (" + f.Value.(string) + ") and operand (" + f.Operand + ") are incompatible")
		}
	}

	return nil
}

func (f *filter) validateOperand(operand string) error {
	if operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
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

func (m *match) Validate() error {
	if err := m.validateOperand(m.Operand); err != nil {
		return err
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

func (m *match) validateOperand(operand string) error {
	if operand == "=" || operand == "<>" {
		return nil
	}

	return errors.New("The operand is invalid.")
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
