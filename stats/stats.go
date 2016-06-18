package stats

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// SetServiceMapper ..
func SetServiceMapper(m func(Service) Service) {
	if m != nil {
		serviceMapper = m
	}
}

var serviceMapper = func(svc Service) Service {
	return svc
}

// SetMapper ..
func SetMapper(m func([]Service) []Service) {
	if m != nil {
		mapper = m
	}
}

var mapper = func(svc []Service) []Service {
	return svc
}

// Service ..
type Service map[string]interface{}

// String ..
func (svc Service) String(key string) (v string) {
	v, _ = svc[key].(string)
	return
}

// Int ..
func (svc Service) Int(key string) (v int64) {
	v, _ = svc[key].(int64)
	return
}

// Services ...
type Services []Service

// Field ..
type Field interface {
	Name() string
	Value(string) (interface{}, error)
}

// FieldMap ..
type FieldMap map[string]Field

// TextField ..
type TextField string

// NumberField ..
type NumberField string

// Name ..
func (f TextField) Name() string {
	return string(f)
}

// Value ..
func (f TextField) Value(v string) (interface{}, error) {
	return v, nil
}

// Name ..
func (f NumberField) Name() string {
	return string(f)
}

// Value ..
func (f NumberField) Value(v string) (interface{}, error) {
	return strconv.ParseInt(v, 10, 64)
}

// Fields ..
var Fields = FieldMap{
	"svname": TextField("svname"),
	"pxname": TextField("pxname"),
}

// New ..
func New(mode, address string, timeout time.Duration) (Services, error) {
	conn, err := net.Dial(mode, address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(timeout))
	fmt.Fprintln(conn, "show stat")

	return FromReader(conn)
}

// FromReader ..
func FromReader(reader io.Reader) (Services, error) {
	reader.Read(make([]byte, 2))
	cr := csv.NewReader(reader)

	names, err := cr.Read()
	if err != nil {
		return nil, err
	}

	var services Services
	for {
		record, err := cr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		service := make(Service)
		for i, v := range record[:len(record)-1] {
			if field, ok := Fields[names[i]]; ok {
				if value, err := field.Value(v); err == nil {
					service[field.Name()] = value
				}
			}
		}

		services = append(services, serviceMapper(service))
	}

	return mapper(services), nil
}
