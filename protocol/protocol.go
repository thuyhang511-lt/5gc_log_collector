package protocol

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type LogRecord struct {
	NF     string
	API    string
	IMSI   string
	TSUnix int64
	Lat    int64
	Status int
}

var ValidAPIs = map[string][]string{
	"AMF": {"RegistrationRequest", "UEContextRelease", "HandoverRequest"},
	"SMF": {"CreateSMContext", "UpdateSMContext", "ReleaseSMContext"},
	"UDM": {"GetSubscriberData", "UpdateSubscriberData"},
	"NEF": {"NotifyEvent", "SubscribeEvent"},
}

func NFList() []string {
	nfs := make([]string, 0, len(ValidAPIs))
	for nf := range ValidAPIs {
		nfs = append(nfs, nf)
	}
	return nfs
}

func Encode(r LogRecord) []byte {
	var b strings.Builder
	b.Grow(96)
	b.WriteString("nf=")
	b.WriteString(r.NF)
	b.WriteString(";api=")
	b.WriteString(r.API)
	b.WriteString(";imsi=")
	b.WriteString(r.IMSI)
	b.WriteString(";ts=")
	b.WriteString(strconv.FormatInt(r.TSUnix, 10))
	b.WriteString(";lat=")
	b.WriteString(strconv.FormatInt(r.Lat, 10))
	b.WriteString(";status=")
	b.WriteString(strconv.Itoa(r.Status))
	b.WriteByte('\n')
	return []byte(b.String())
}

func Parse(line []byte) (LogRecord, error) {
	var r LogRecord
	s := string(line)
	for s != "" {
		var field string
		field, s, _ = strings.Cut(s, ";")
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			return r, fmt.Errorf("protocol: invalid field %q", field)
		}
		switch key {
		case "nf":
			r.NF = val
		case "api":
			r.API = val
		case "imsi":
			r.IMSI = val
		case "ts":
			ts, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid ts %q: %w", val, err)
			}
			r.TSUnix = ts
		case "lat":
			lat, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid lat %q: %w", val, err)
			}
			r.Lat = lat
		case "status":
			code, err := strconv.Atoi(val)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid status %q: %w", val, err)
			}
			r.Status = code
		default:
			// bỏ qua field lạ để không phải nâng version protocol
			// mỗi khi thêm field mới không bắt buộc
		}
	}
	if r.NF == "" || r.API == "" {
		return r, fmt.Errorf("protocol: missing nf/api in line %q", line)
	}
	return r, nil
}

func NowMillis() int64 {
	return time.Now().UnixMilli()
}
