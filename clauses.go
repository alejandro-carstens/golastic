package golastic

import "errors"

type whereIn struct {
	Field  string
	Values []interface{}
}

func (wi *whereIn) Validate() error {
	for _, value := range wi.Values {
		if !IsNumeric(value) && !IsString(value) {
			return errors.New("The value is not numeric nor a string.")
		}
	}

	return nil
}

type whereNotIn struct {
	whereIn
}

func (wni *whereNotIn) New(field string, values []interface{}) *whereNotIn {
	wni.Field = field
	wni.Values = values

	return wni
}

type where struct {
	field   string
	operand string
	value   interface{}
}

func (w *where) New(field string, operand string, value interface{}) *where {
	w.setField(field).setOperand(operand).setValue(value)

	return w
}

func (w *where) Validate() error {
	if err := w.validateOperand(w.operand); err != nil {
		return err
	}

	if !IsNumeric(w.value) {
		if !IsString(w.value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if w.operand != "=" && w.operand != "<>" {
			return errors.New("Value (" + w.value.(string) + ") and operand (" + w.operand + ") are incompatible")
		}
	}

	return nil
}

func (w *where) IsString() bool {
	return IsString(w.value)
}

func (w *where) IsDate() bool {
	return IsDate(w.value)
}

func (w *where) validateOperand(operand string) error {
	if operand == "<>" || operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

func (w *where) setField(field string) *where {
	w.field = field

	return w
}

func (w *where) setOperand(operand string) *where {
	w.operand = operand

	return w
}

func (w *where) setValue(value interface{}) *where {
	w.value = value

	return w
}

func (w *where) GetField() string {
	return w.field
}

func (w *where) GetOperand() string {
	return w.operand
}

func (w *where) GetValue() interface{} {
	return w.value
}

type Filter struct {
	where
}

func (f *Filter) New(field string, operand string, value interface{}) *Filter {
	f.setField(field).setOperand(operand).setValue(value)

	return f
}

func (f *Filter) Validate() error {
	if err := f.validateOperand(f.operand); err != nil {
		return err
	}

	if !IsNumeric(f.value) {
		if !IsString(f.value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if f.operand != "=" {
			return errors.New("Value (" + f.value.(string) + ") and operand (" + f.operand + ") are incompatible")
		}
	}

	return nil
}

func (f *Filter) validateOperand(operand string) error {
	if operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

type FilterIn struct {
	whereIn
}

func (fi *FilterIn) New(field string, values []interface{}) *FilterIn {
	fi.Field = field
	fi.Values = values

	return fi
}

type Match struct {
	where
}

func (m *Match) New(field string, operand string, value interface{}) *Match {
	m.setField(field).setOperand(operand).setValue(value)

	return m
}

func (m *Match) setField(field string) *Match {
	m.field = field

	return m
}

func (m *Match) setOperand(operand string) *Match {
	m.operand = operand

	return m
}

func (m *Match) setValue(value interface{}) *Match {
	m.value = value

	return m
}

func (m *Match) Validate() error {
	if err := m.validateOperand(m.operand); err != nil {
		return err
	}

	if !IsNumeric(m.value) {
		if !IsString(m.value) {
			return errors.New("The value is not numeric nor a string.")
		}

		if m.operand != "=" && m.operand != "<>" {
			return errors.New("Value (" + m.value.(string) + ") and operand (" + m.operand + ") are fucking incompatible")
		}
	}

	return nil
}

func (m *Match) validateOperand(operand string) error {
	if operand == "=" || operand == "<>" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

type MatchIn struct {
	whereIn
}

func (mi *MatchIn) New(field string, values []interface{}) *MatchIn {
	mi.Field = field
	mi.Values = values

	return mi
}

type MatchNotIn struct {
	whereIn
}

func (mni *MatchNotIn) New(field string, values []interface{}) *MatchNotIn {
	mni.Field = field
	mni.Values = values

	return mni
}

type Sort struct {
	field string
	order bool
}

func (s *Sort) New(field string, asc bool) *Sort {
	s.setField(field)
	s.setOrder(asc)

	return s
}

func (s *Sort) setField(field string) *Sort {
	s.field = field

	return s
}

func (s *Sort) setOrder(asc bool) *Sort {
	s.order = asc

	return s
}

func (s *Sort) GetOrder() bool {
	return s.order
}

func (s *Sort) GetField() string {
	return s.field
}

type Limit struct {
	limit int
}

func (l *Limit) New(limit int) *Limit {
	l.limit = limit

	return l
}

func (l *Limit) GetLimit() int {
	return l.limit
}

type From struct {
	from int
}

func (f *From) New(from int) *From {
	f.from = from

	return f
}

func (f *From) GetFrom() int {
	return f.from
}

type GroupBy struct {
	fields []string
}

func (g *GroupBy) New(fields []string) *GroupBy {
	g.fields = fields

	return g
}

func (g *GroupBy) GetFields() []string {
	return g.fields
}
