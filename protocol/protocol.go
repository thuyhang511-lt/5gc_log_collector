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
	Location  string
	Latency   int64
	Status    int
}

var ValidAPIs = map[string][]string{
	"AMF": {"registration", "n1n2message_transfer", "ue_context_transfer", "ue_authentication"},
	"SMF": {"sm_context_create", "sm_context_update", "sm_context_release", "pdu_session_modify"},
	"NEF": {"monitoring_event_subscribe", "monitoring_event_unsubscribe", "pfd_management"},
	"UDM": {"subscriber_data_get", "ue_context_update", "subscription_data_subscribe"},
}

var networkFunctions = []string{"AMF", "SMF", "NEF", "UDM"}

const timeLayout = time.RFC3339

func NFList() []string {
	return append([]string(nil), networkFunctions...)
}

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
	b.WriteString(";location=")
	b.WriteString(r.Location)
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
	input := string(normalized)

	var hasTimestamp, hasNF, hasAPI, hasIMSI, hasLocation, hasLatency, hasStatus bool

	for input != "" {
		var field string
		field, input, _ = strings.Cut(input, ";")
		if field == "" {
			continue
		}

		key, value, ok := strings.Cut(field, "=")
		if !ok || key == "" || value == "" {
			return r, fmt.Errorf("protocol: invalid field %q", field)
		}

		switch key {
		case "timestamp":
			if hasTimestamp {
				return r, fmt.Errorf("protocol: duplicate timestamp")
			}
			timestamp, err := time.Parse(timeLayout, value)
			if err != nil {
				return r, fmt.Errorf("protocol: invalid timestamp %q: %w", value, err)
			}
			r.Timestamp = timestamp
			hasTimestamp = true

		case "nf":
			if hasNF {
				return r, fmt.Errorf("protocol: duplicate nf")
			}
			r.NF = value
			hasNF = true

		case "api":
			if hasAPI {
				return r, fmt.Errorf("protocol: duplicate api")
			}
			r.API = value
			hasAPI = true

		case "imsi":
			if hasIMSI {
				return r, fmt.Errorf("protocol: duplicate imsi")
			}
			if !validIMSI(value) {
				return r, fmt.Errorf("protocol: invalid imsi %q", value)
			}
			r.IMSI = value
			hasIMSI = true

		case "location":
			if hasLocation {
				return r, fmt.Errorf("protocol: duplicate location")
			}
			r.Location = value
			hasLocation = true

		case "latency":
			if hasLatency {
				return r, fmt.Errorf("protocol: duplicate latency")
			}
			latency, err := strconv.ParseInt(value, 10, 64)
			if err != nil || latency < 0 {
				return r, fmt.Errorf("protocol: invalid latency %q", value)
			}
			r.Latency = latency
			hasLatency = true

		case "status":
			if hasStatus {
				return r, fmt.Errorf("protocol: duplicate status")
			}
			status, err := strconv.Atoi(value)
			if err != nil || status < 100 || status > 599 {
				return r, fmt.Errorf("protocol: invalid status %q", value)
			}
			r.Status = status
			hasStatus = true

		default:
			return r, fmt.Errorf("protocol: unknown field %q", key)
		}
	}

	if !hasTimestamp || !hasNF || !hasAPI || !hasIMSI || !hasLocation || !hasLatency || !hasStatus {
		return r, fmt.Errorf("protocol: missing required field")
	}
	if !validNFAPI(r.NF, r.API) {
		return r, fmt.Errorf("protocol: api %q is not valid for nf %q", r.API, r.NF)
	}

	return r, nil
}

func validIMSI(imsi string) bool {
	if len(imsi) != 15 {
		return false
	}
	for i := 0; i < len(imsi); i++ {
		if imsi[i] < '0' || imsi[i] > '9' {
			return false
		}
	}
	return true
}

func validNFAPI(nf, api string) bool {
	apis, ok := ValidAPIs[nf]
	if !ok {
		return false
	}
	for _, allowed := range apis {
		if api == allowed {
			return true
		}
	}
	return false
}

func Now() time.Time {
	return time.Now().UTC()
}
