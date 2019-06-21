package golastic

import "errors"

type WhereIn struct {
	field  string
	values []interface{}
}

func (wi *WhereIn) New(field string, values []interface{}) *WhereIn {
	wi.field = field
	wi.values = values

	return wi
}

func (wi *WhereIn) GetField() string {
	return wi.field
}

func (wi *WhereIn) GetValues() []interface{} {
	return wi.values
}

func (wi *WhereIn) Validate() error {
	for _, value := range wi.values {
		if !IsNumeric(value) && !IsString(value) {
			return errors.New("The value is not numeric nor a string.")
		}
	}

	return nil
}

type WhereNotIn struct {
	WhereIn
}

func (wni *WhereNotIn) New(field string, values []interface{}) *WhereNotIn {
	wni.field = field
	wni.values = values

	return wni
}

type Where struct {
	field   string
	operand string
	value   interface{}
}

func (w *Where) New(field string, operand string, value interface{}) *Where {
	w.setField(field).setOperand(operand).setValue(value)

	return w
}

func (w *Where) Validate() error {
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

func (w *Where) IsString() bool {
	return IsString(w.value)
}

func (w *Where) IsDate() bool {
	return IsDate(w.value)
}

func (w *Where) validateOperand(operand string) error {
	if operand == "<>" || operand == "=" || operand == ">" || operand == "<" || operand == "<=" || operand == ">=" {
		return nil
	}

	return errors.New("The operand is invalid.")
}

func (w *Where) setField(field string) *Where {
	w.field = field

	return w
}

func (w *Where) setOperand(operand string) *Where {
	w.operand = operand

	return w
}

func (w *Where) setValue(value interface{}) *Where {
	w.value = value

	return w
}

func (w *Where) GetField() string {
	return w.field
}

func (w *Where) GetOperand() string {
	return w.operand
}

func (w *Where) GetValue() interface{} {
	return w.value
}

type Filter struct {
	Where
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
	WhereIn
}

func (fi *FilterIn) New(field string, values []interface{}) *FilterIn {
	fi.field = field
	fi.values = values

	return fi
}

type Match struct {
	Where
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
	WhereIn
}

func (mi *MatchIn) New(field string, values []interface{}) *MatchIn {
	mi.field = field
	mi.values = values

	return mi
}

type MatchNotIn struct {
	WhereIn
}

func (mni *MatchNotIn) New(field string, values []interface{}) *MatchNotIn {
	mni.field = field
	mni.values = values

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
