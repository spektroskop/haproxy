package stats

import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

var transformers []func(Service) Service

// AddTransformer ..
func AddTransformer(transformer func(Service) Service) {
	transformers = append(transformers, transformer)
}

// Service ...
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
var Fields = map[string]Field{
	"svname":         TextField("name"),
	"pxname":         TextField("proxy"),
	"qcur":           NumberField("currentQueued"),
	"scur":           NumberField("currentSessions"),
	"rate":           NumberField("sessionRate"),
	"qtime":          NumberField("queueTime"),
	"ctime":          NumberField("connectTime"),
	"rtime":          NumberField("responseTime"),
	"bin":            NumberField("received"),
	"bout":           NumberField("sent"),
	"req_rate":       NumberField("httpRequestRate"),
	"hrsp_1xx":       NumberField("httpResponse1xx"),
	"hrsp_2xx":       NumberField("httpResponse2xx"),
	"hrsp_3xx":       NumberField("httpResponse3xx"),
	"hrsp_4xx":       NumberField("httpResponse4xx"),
	"hrsp_5xx":       NumberField("httpResponse5xx"),
	"hrsp_other":     NumberField("httpResponseOther"),
	"status":         TextField("status"),
	"weight":         NumberField("weight"),
	"lbtot":          NumberField("selected"),
	"lastchg":        NumberField("lastChange"),
	"lastsess":       NumberField("lastSession"),
	"downtime":       NumberField("downtime"),
	"check_status":   TextField("checkStatus"),
	"check_duration": NumberField("checkDuration"),
	"check_code":     NumberField("checkCode"),
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

		for _, transform := range transformers {
			service = transform(service)
		}

		services = append(services, service)
	}

	return services, nil
}
