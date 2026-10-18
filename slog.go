package simpledatalogs

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// identityAttrKeys are top-level attributes that fill identity fields instead of props.
var identityAttrKeys = map[string]func(string) Option{
	"user_id":      WithUserID,
	"device_id":    WithDeviceID,
	"session_id":   WithSessionID,
	"anonymous_id": WithAnonymousID,
}

// HandlerOptions configure the slog Handler.
type HandlerOptions struct {
	// Level is the minimum level shipped (default slog.LevelInfo).
	Level slog.Leveler
}

// Handler is a slog.Handler that queues each record on a Client as a log line.
type Handler struct {
	client      *Client
	minLevel    slog.Leveler
	groupPrefix string
	props       Props
	options     []Option
}

// NewHandler returns a slog.Handler for the client; attributes become props, groups become dotted names.
func NewHandler(client *Client, handlerOptions *HandlerOptions) *Handler {
	var minLevel slog.Leveler = slog.LevelInfo

	if handlerOptions != nil && handlerOptions.Level != nil {
		minLevel = handlerOptions.Level
	}

	return &Handler{client: client, minLevel: minLevel, props: Props{}}
}

// Enabled reports whether records at this level are shipped.
func (handler *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return handler.client != nil && level >= handler.minLevel.Level()
}

// Handle queues the record with its level, time, message and attributes; it never returns an error.
func (handler *Handler) Handle(_ context.Context, record slog.Record) error {
	props := make(Props, len(handler.props)+record.NumAttrs())
	options := append([]Option(nil), handler.options...)

	for propName, propValue := range handler.props {
		props[propName] = propValue
	}

	// The SDK's own warnings are never shipped, so a misconfigured logger can't loop.
	isInternal := false

	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == internalAttrKey {
			isInternal = true
			return false
		}

		options = addAttr(props, options, handler.groupPrefix, attr)
		return true
	})

	if isInternal {
		return nil
	}

	if !record.Time.IsZero() {
		options = append(options, WithTime(record.Time))
	}

	handler.client.Log(levelForRecord(record.Level), record.Message, props, options...)

	return nil
}

// WithAttrs returns a handler whose records carry these attributes too.
func (handler *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	derived := handler.clone()

	for _, attr := range attrs {
		derived.options = addAttr(derived.props, derived.options, derived.groupPrefix, attr)
	}

	return derived
}

// WithGroup returns a handler that nests later attributes under the group's name.
func (handler *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return handler
	}

	derived := handler.clone()
	derived.groupPrefix = handler.groupPrefix + name + "."

	return derived
}

// clone copies the handler, so derived handlers never share their props.
func (handler *Handler) clone() *Handler {
	copiedProps := make(Props, len(handler.props))

	for propName, propValue := range handler.props {
		copiedProps[propName] = propValue
	}

	return &Handler{
		client:      handler.client,
		minLevel:    handler.minLevel,
		groupPrefix: handler.groupPrefix,
		props:       copiedProps,
		options:     append([]Option(nil), handler.options...),
	}
}

// levelForRecord maps a slog level onto the ingest API's; Error+4 and above is critical.
func levelForRecord(level slog.Level) Level {
	switch {
	case level < slog.LevelInfo:
		return LevelDebug
	case level < slog.LevelWarn:
		return LevelInfo
	case level < slog.LevelError:
		return LevelWarning
	case level < slog.LevelError+4:
		return LevelError
	default:
		return LevelCritical
	}
}

// addAttr flattens one attribute into props (groups become dotted names); top-level identity keys become options.
func addAttr(props Props, options []Option, groupPrefix string, attr slog.Attr) []Option {
	attr.Value = attr.Value.Resolve()

	if attr.Equal(slog.Attr{}) {
		return options
	}

	// A group nests its attributes under its name; an unnamed group inlines them.
	if attr.Value.Kind() == slog.KindGroup {
		nestedPrefix := groupPrefix

		if attr.Key != "" {
			nestedPrefix = groupPrefix + attr.Key + "."
		}

		for _, nestedAttr := range attr.Value.Group() {
			options = addAttr(props, options, nestedPrefix, nestedAttr)
		}

		return options
	}

	// Identity fields at the top level fill the line's identity.
	if makeOption, isIdentity := identityAttrKeys[attr.Key]; isIdentity && groupPrefix == "" {
		return append(options, makeOption(attr.Value.String()))
	}

	props[groupPrefix+attr.Key] = attrPropValue(attr.Value)

	return options
}

// attrPropValue is a slog value as a prop: scalars as they are, times as RFC 3339, errors and others as text.
func attrPropValue(value slog.Value) any {
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().String()
	case slog.KindTime:
		return value.Time().Format(time.RFC3339Nano)
	}

	// Errors and Stringers read as their text; anything else is formatted.
	switch typedValue := value.Any().(type) {
	case error:
		return typedValue.Error()
	case fmt.Stringer:
		return typedValue.String()
	}

	if scalarValue, isScalar := scalarPropValue(value.Any()); isScalar {
		return scalarValue
	}

	return fmt.Sprint(value.Any())
}
