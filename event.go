package simpledatalogs

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

// maxProps is the ingest API's limit on props per event.
const maxProps = 100

// truncatedMessageBytes is the most of a message an oversized log line keeps.
const truncatedMessageBytes = 8192

// tsLayout is ISO 8601 with milliseconds and an offset.
const tsLayout = "2006-01-02T15:04:05.000Z07:00"

// Props are an event's chartable values: strings, finite numbers and booleans.
type Props map[string]any

// Level is a log line's severity.
type Level string

// The ingest API's levels, in rising severity.
const (
	LevelDebug    Level = "debug"
	LevelInfo     Level = "info"
	LevelNotice   Level = "notice"
	LevelWarning  Level = "warning"
	LevelError    Level = "error"
	LevelCritical Level = "critical"
)

// levelAliases maps common spellings onto the ingest API's levels.
var levelAliases = map[string]Level{
	"trace":       LevelDebug,
	"information": LevelInfo,
	"warn":        LevelWarning,
	"err":         LevelError,
	"crit":        LevelCritical,
	"fatal":       LevelCritical,
	"alert":       LevelCritical,
	"emergency":   LevelCritical,
}

// Option sets one per-call field: an identity or the time it happened.
type Option func(*callOptions)

// callOptions are the per-call fields an Option can set.
type callOptions struct {
	happenedAt  time.Time
	userID      string
	deviceID    string
	sessionID   string
	anonymousID string
}

// WithUserID sets the line's user_id.
func WithUserID(userID string) Option {
	return func(options *callOptions) { options.userID = strings.TrimSpace(userID) }
}

// WithDeviceID sets the line's device_id.
func WithDeviceID(deviceID string) Option {
	return func(options *callOptions) { options.deviceID = strings.TrimSpace(deviceID) }
}

// WithSessionID sets the line's session_id.
func WithSessionID(sessionID string) Option {
	return func(options *callOptions) { options.sessionID = strings.TrimSpace(sessionID) }
}

// WithAnonymousID sets the line's anonymous_id.
func WithAnonymousID(anonymousID string) Option {
	return func(options *callOptions) { options.anonymousID = strings.TrimSpace(anonymousID) }
}

// WithTime sets when the line happened; by default it is the moment of the call.
func WithTime(happenedAt time.Time) Option {
	return func(options *callOptions) { options.happenedAt = happenedAt }
}

// wireEvent is one NDJSON line as the ingest API reads it.
type wireEvent struct {
	Event       string         `json:"event,omitempty"`
	EventID     string         `json:"event_id"`
	TS          string         `json:"ts"`
	Level       Level          `json:"level,omitempty"`
	Message     *string        `json:"message,omitempty"`
	UserID      string         `json:"user_id,omitempty"`
	DeviceID    string         `json:"device_id,omitempty"`
	SessionID   string         `json:"session_id,omitempty"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	Props       map[string]any `json:"props,omitempty"`
}

// newWireEvent stamps the id and time now, so a retry counts once, and applies the options.
func newWireEvent(options []Option) wireEvent {
	resolved := callOptions{}

	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}

	happenedAt := resolved.happenedAt

	if happenedAt.IsZero() {
		happenedAt = time.Now()
	}

	return wireEvent{
		EventID:     newEventID(),
		TS:          happenedAt.UTC().Format(tsLayout),
		UserID:      resolved.userID,
		DeviceID:    resolved.deviceID,
		SessionID:   resolved.sessionID,
		AnonymousID: resolved.anonymousID,
	}
}

// newEventID is a random UUID (version 4) from the system's CSPRNG.
func newEventID() string {
	var randomBytes [16]byte

	// crypto/rand never fails on supported platforms; a zero id would still be sent.
	_, _ = rand.Read(randomBytes[:])

	// Set the version (4) and variant (10xx) bits.
	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", randomBytes[0:4], randomBytes[4:6], randomBytes[6:8], randomBytes[8:10], randomBytes[10:16])
}

// normalizeLevel maps common spellings onto the ingest API's levels; unknown names pass through lowercased.
func normalizeLevel(level Level) Level {
	levelText := strings.ToLower(strings.TrimSpace(string(level)))

	if alias, isAlias := levelAliases[levelText]; isAlias {
		return alias
	}

	return Level(levelText)
}

// scalarPropValue is the value as a string, finite number or boolean, and false when it can't be one.
func scalarPropValue(propValue any) (any, bool) {
	switch typedValue := propValue.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return typedValue, true
	case float32:
		return typedValue, !math.IsNaN(float64(typedValue)) && !math.IsInf(float64(typedValue), 0)
	case float64:
		return typedValue, !math.IsNaN(typedValue) && !math.IsInf(typedValue, 0)
	case fmt.Stringer:
		return typedValue.String(), true
	default:
		return namedScalarValue(propValue)
	}
}

// namedScalarValue unwraps named types over scalars (type Plan string) to their underlying value.
func namedScalarValue(propValue any) (any, bool) {
	reflected := reflect.ValueOf(propValue)

	switch reflected.Kind() {
	case reflect.String:
		return reflected.String(), true
	case reflect.Bool:
		return reflected.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return reflected.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflected.Uint(), true
	case reflect.Float32, reflect.Float64:
		return scalarPropValue(reflected.Float())
	default:
		return nil, false
	}
}

// cleanProps keeps scalar props up to the API's limit, in name order, reporting the kinds it skipped.
func cleanProps(props Props, warn func(kind string, message string)) map[string]any {
	keptProps := make(map[string]any, len(props))

	// Sorted names make the 100-prop cut the same on every call.
	propNames := make([]string, 0, len(props))

	for propName := range props {
		propNames = append(propNames, propName)
	}

	sort.Strings(propNames)

	for _, propName := range propNames {
		keptValue, isScalar := scalarPropValue(props[propName])

		if !isScalar {
			warn("prop_value", fmt.Sprintf("prop %q is not a string, finite number or boolean; it was skipped", propName))
			continue
		}

		if len(keptProps) >= maxProps {
			warn("prop_count", fmt.Sprintf("an event had more than %d props; the rest were skipped", maxProps))
			break
		}

		keptProps[propName] = keptValue
	}

	return keptProps
}

// mergeProps puts the defaults under the call's props, within the API's limit.
func mergeProps(defaultProps map[string]any, callProps map[string]any) map[string]any {
	for propName, propValue := range defaultProps {
		if _, isSet := callProps[propName]; !isSet && len(callProps) < maxProps {
			callProps[propName] = propValue
		}
	}

	if len(callProps) == 0 {
		return nil
	}

	return callProps
}

// encodeLine marshals one line without HTML escaping; invalid UTF-8 becomes U+FFFD.
func encodeLine(event wireEvent) ([]byte, error) {
	var encoded bytes.Buffer

	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)

	if err := encoder.Encode(event); err != nil {
		return nil, err
	}

	// The encoder ends with a newline; the queue adds its own.
	return bytes.TrimRight(encoded.Bytes(), "\n"), nil
}

// truncatedLogLine rebuilds an oversized log line with a cut message and no props, saying how much was dropped.
func truncatedLogLine(event wireEvent, droppedBytes int, maxLineBytes int) ([]byte, error) {
	keptMessageBytes := max(min(truncatedMessageBytes, maxLineBytes-1024), 0)
	message := ""

	if event.Message != nil {
		message = *event.Message
	}

	// Cut on a rune boundary, so the message stays valid UTF-8.
	if len(message) > keptMessageBytes {
		message = strings.ToValidUTF8(message[:keptMessageBytes], "")
	}

	event.Message = &message
	event.Props = map[string]any{"_truncated": true, "_dropped_bytes": droppedBytes}

	return encodeLine(event)
}
