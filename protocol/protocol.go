package protocol

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type LogRecord struct {
	Timestamp time.Time
	NF        string
	API       string
	IMSI      string
	Latency   int64
	Status    int
}

var ValidAPIs = map[string][]string{
	"AMF": {"registration", "n1n2message_transfer", "ue_context_transfer", "ue_authentication"},
	"SMF": {"sm_context_create", "sm_context_update", "sm_context_release", "pdu_session_modify"},
	"NEF": {"monitoring_event_subscribe", "monitoring_event_unsubscribe", "pfd_management"},
	"UDM": {"subscriber_data_get", "ue_context_update", "subscription_data_subscribe"},
}

func NFList() []string {
	nfs := make([]string, 0, len(ValidAPIs))
	for nf := range ValidAPIs {
		nfs = append(nfs, nf)
	}
	return nfs
}

const timeLayout = time.RFC3339

func Encode(r LogRecord) []byte {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("timestamp=")
	b.WriteString(r.Timestamp.UTC().Format(timeLayout))
	b.WriteString(" nf=")
	b.WriteString(r.NF)
	b.WriteString(";api=")
	b.WriteString(r.API)
	b.WriteString(";imsi=")
	b.WriteString(r.IMSI)
	b.WriteString(" latency=")
	b.WriteString(strconv.FormatInt(r.Latency, 10))
	b.WriteString(";status=")
	b.WriteString(strconv.Itoa(r.Status))
	b.WriteByte('\n')
	return []byte(b.String())
}

func Parse(line []byte) (LogRecord, error) {
	var r LogRecord
	normalized := bytes.Join(bytes.Fields(line), []byte(";"))
	s := string(normalized)

	for s != "" {
		var field string
		field, s, _ = strings.Cut(s, ";")
		if field == "" {
			continue
		}
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			return r, fmt.Errorf("protocol: invalid field %q", field)
		}
		switch key {
		case "timestamp":
			ts, err := time.Parse(timeLayout, val)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid timestamp %q: %w", val, err)
			}
			r.Timestamp = ts
		case "nf":
			r.NF = val
		case "api":
			r.API = val
		case "imsi":
			r.IMSI = val
		case "latency":
			lat, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid latency %q: %w", val, err)
			}
			r.Latency = lat
		case "status":
			code, err := strconv.Atoi(val)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid status %q: %w", val, err)
			}
			r.Status = code
		default:

		}
	}
	if r.NF == "" || r.API == "" {
		return r, fmt.Errorf("protocol: missing nf/api in line %q", line)
	}
	return r, nil
}

func Now() time.Time {
	return time.Now().UTC()
}
