package internal

// ConsolidateErr turns many errors into a single error, filtering out nil errors
func ConsolidateErr(err []error) error {
	if len(err) == 0 {
		return nil
	}
	if len(err) == 1 {
		return err[0]
	}
	return &multiErr{err: err}
}

type multiErr struct {
	err []error
}

var _ error = &multiErr{}

func (m *multiErr) Error() string {
	ret := "Multiple errors: "
	for _, e := range m.err {
		ret += e.Error()
	}
	return ret
}
